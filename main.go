package avroma

import (
	"context"
	"encoding/binary"
	"github.com/alexsergivan/avroma/cache"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	schemaregistry "github.com/landoop/schema-registry"
	"github.com/linkedin/goavro/v2"
)

func init() {
	// Default Options.
	c = &Options{
		Version:              "2.2.1",
		Oldest:               true,
		MarkMessage:          true,
		Verbose:              false,
		AvroSchema:           cache.NewData(),
	}
}

// Message contains decoded/decompressed avro messages.
type Message struct {
	SchemaID  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

var c *Options

// DataFlow starts sarama consumer data flow.
func DataFlow(fn func(Message) error, setters ...Option) {
	for _, setter := range setters {
		setter(c)
	}



	log.Println("Starting a new Sarama consumer")

	if c.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	if c.Oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
		fn:    fn,
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(c.Brokers, c.Group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, c.Topics, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
	fn    func(Message) error
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		//log.Printf("Message | Key = %s | timestamp = %v | topic = %s", message.Key, message.Timestamp, message.Topic)
		msg, _ := processAvroMsg(message)
		err := consumer.fn(msg)
		if c.MarkMessage && err == nil {
			// Mark message after is consumed.
			session.MarkMessage(message, "")
		}
	}
	return nil
}

//processAvroMsg Processing avro messages
func processAvroMsg(m *sarama.ConsumerMessage) (Message, error) {
	schemaID := binary.BigEndian.Uint32(m.Value[1:5])
	schema := c.AvroSchema.Get(schemaID)

	if schema == nil {
		log.Printf("Avro schemaID: %d not found in cache getting from registry", schemaID)
		schemaRegistryClient := schemaRegistryClientInit(c.SchemaRegistryClientUrl)
		sc, err := schemaRegistryClient.GetSchemaByID(int(schemaID))
		if err != nil {
			log.Panicf("Error getting SchemaID %d: %v", schemaID, err)
		}
		c.AvroSchema.Add(schemaID, sc)
	}
	schema = c.AvroSchema.Get(schemaID)

	codec, err := goavro.NewCodec(schema.(string))
	if err != nil {
		//log.Printf("Codec ERR: %v", err)
		return Message{}, err
	}

	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		//log.Printf("Native ERR: %v", err)
		return Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)
	if err != nil {
		//log.Printf("Native ERR: %v", err)
		return Message{}, err
	}
	msg := Message{int(schemaID), m.Topic, m.Partition, m.Offset, string(m.Key), string(textual)}
	//log.Printf("MSG = %s", msg.Value)
	return msg, nil
}

func schemaRegistryClientInit(url string) (c *schemaregistry.Client) {
	client, err := schemaregistry.NewClient(url)

	if err != nil {
		log.Panicf("Error creating schemaregistry client: %v", err)
	}
	subjects, err := client.Subjects()
	if err != nil {
		log.Panicf("Error getting subjects: %v", err)
	}
	log.Printf("Found %d subjects in %s ...", len(subjects), url)
	return client
}
