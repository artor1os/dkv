package test

import (
	"testing"

	"github.com/artor1os/dkv/util"
)

type cli struct {
	host string
}

func (c *cli) Get(t *testing.T, key string) string {
	var value string
	if err := util.Get(host+"/get", map[string]string{"key": key}, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (c *cli) Put(t *testing.T, key string, value string) {
	if err := util.Get(host+"/put", map[string]string{"key": key, "value": value}, nil); err != nil {
		t.Fatal(err)
	}
}

func (c *cli) Append(t *testing.T, key string, value string) {
	if err := util.Get(host+"/append", map[string]string{"key": key, "value": value}, nil); err != nil {
		t.Fatal(err)
	}
}

func (c *cli) Delete(t *testing.T, key string) string {
	var value string
	if err := util.Get(host+"/delete", map[string]string{"key": key}, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

const host1 = "http://localhost:8081"

func newCli(host string) *cli {
	return &cli{host: host}
}

func TestMultiClientConcurrent(t *testing.T) {
	c := newCli(host)
	c1 := newCli(host1)

	key := String(5)
	value := String(5)
	value1 := String(5)

	done := make(chan bool)
	go func() {
		c.Put(t, key, value)
		done <- true
	}()
	go func() {
		c1.Put(t, key, value1)
		done <- true
	}()

	<-done
	<-done

	actual := c.Get(t, key)
	actual1 := c1.Get(t, key)
	check(t, actual, actual1)
}
