package avroma

import (
	"github.com/alexsergivan/avroma/cache"
)

// Options avroma configuration options.
type Options struct {
	Brokers              []string
	Version              string
	Group                string
	Topics               []string
	Oldest               bool
	Verbose              bool
	MarkMessage          bool
	AvroSchema           cache.Data
	SchemaRegistryClientUrl string
}

// Option pointer function to configuration options.
type Option func(*Options)

// Brokers configures sarama brokers.
func Brokers(brokers []string) Option {
	return func(args *Options) {
		args.Brokers = brokers
	}
}

// Version configures kafka version.
func Version(version string) Option {
	return func(args *Options) {
		args.Version = version
	}
}

// Group configures consumer group for kafka.
func Group(group string) Option {
	return func(args *Options) {
		args.Group = group
	}
}

// Topics configures kafka topics to follow.
func Topics(topics []string) Option {
	return func(args *Options) {
		args.Topics = topics
	}
}

// Oldest configures to start from oldest message or not.
func Oldest(oldest bool) Option {
	return func(args *Options) {
		args.Oldest = oldest
	}
}

// Verbose configures sarama verbosity logs.
func Verbose(verbose bool) Option {
	return func(args *Options) {
		args.Verbose = verbose
	}
}

// MarkMessage configures sarama consumer to mark consumed messages or not.
func MarkMessage(markm bool) Option {
	return func(args *Options) {
		args.MarkMessage = markm
	}
}

// AvroSchema configures place to cache avro schemas.
func AvroSchema(avs cache.Data) Option {
	return func(args *Options) {
		args.AvroSchema = avs
	}
}

// SchemaRegistryClientUrl configures github.com/landoop/schema-registry client.
func SchemaRegistryClientUrl(url string) Option {
	return func(args *Options) {
		args.SchemaRegistryClientUrl = url
	}
}
