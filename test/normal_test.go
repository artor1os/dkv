package test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/artor1os/dkv/util"
)

const host = "http://localhost:8080"

const joinURL = host + "/join"
const leaveURL = host + "/leave"
const putURL = host + "/put"
const getURL = host + "/get"
const appendURL = host + "/append"
const deleteURL = host + "/delete"

func check(t *testing.T, expected string, actual string) {
	if actual != expected {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func String(length int) string {
	return StringWithCharset(length, charset)
}

func join(t *testing.T, gid int, peers int) {
	if err := util.Post(joinURL, map[int]int{gid: peers}, nil); err != nil {
		t.Fatal(err)
	}
}

func leave(t *testing.T, gid int) {
	if err := util.Post(leaveURL, []int{gid}, nil); err != nil {
		t.Fatal(err)
	}
}

func put(t *testing.T, key string, value string) {
	if err := util.Get(putURL, map[string]string{"key": key, "value": value}, nil); err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, key string) string {
	var value string
	if err := util.Get(getURL, map[string]string{"key": key}, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func append1(t *testing.T, key string, value string) {
	if err := util.Get(appendURL, map[string]string{"key": key, "value": value}, nil); err != nil {
		t.Fatal(err)
	}
}

func delete1(t *testing.T, key string) string {
	var value string
	if err := util.Get(deleteURL, map[string]string{"key": key}, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestMulti(t *testing.T) {
	for i := 0; i < 100; i++ {
		t.Run(fmt.Sprintf("%v", i), TestPutGetAppend)
	}
}

func TestPutGetAppend(t *testing.T) {
	key := String(5)
	value := String(5)
	put(t, key, value)
	actual := get(t, key)
	check(t, value, actual)

	appendValue := String(5)
	value += appendValue
	append1(t, key, appendValue)
	actual = get(t, key)
	check(t, value, actual)
}

func TestJoinLeave(t *testing.T) {
	key := String(5)
	value := String(5)
	put(t, key, value)
	actual := get(t, key)
	check(t, value, actual)

	leave(t, 100)
	join(t, 100, 3)
	leave(t, 101)
	join(t, 101, 3)

	actual = get(t, key)
	check(t, value, actual)
}

func TestConcurrentJoinLeave(t *testing.T) {
	n := 10
	local := make(map[string]string)
	done := make(chan bool)
	go func() {
		for i := 0; i < n; i++ {
			key := String(5)
			value := String(5)
			local[key] = value
			put(t, key, value)
		}
		done <- true
	}()
	leave(t, 100)
	join(t, 100, 3)
	leave(t, 101)
	join(t, 101, 3)

	<-done
	for k, v := range local {
		actual := get(t, k)
		check(t, v, actual)
	}
}
