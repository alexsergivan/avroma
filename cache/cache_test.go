package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var d = NewData()

func TestData_Add(t *testing.T) {
	d.Add(10, "boo")
	d.Add(10, "foo")
	d.Add(20, "bar")
	for i := 0; i <= 20; i++ {
		d.Add(30, "baz")
	}

	assert.Equal(t, "foo", d.Get(10))
	assert.Equal(t, "bar", d.Get(20))
	assert.Equal(t, "baz", d.Get(30))
}

func TestData_Del(t *testing.T) {
	d.Del(20)
	d.Del(30)
	assert.Equal(t, nil, d.Get(20))
}

func TestData_Kill(t *testing.T) {
	d.Kill(10)
	assert.Equal(t, nil, d.Get(10))

}

func TestData_Dump(t *testing.T) {
	assert.Equal(t, "ValueMap: map[30:baz] | CountMap: map[30:20]", d.Dump())
	//t.Logf(d.Dump())
}
