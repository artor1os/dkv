package test

import (
	"testing"

	"github.com/artor1os/dkv/util"
)

const host = "http://localhost:8080"

const joinURL = host + "/join"
const leaveURL = host + "/leave"
const putURL = host + "/put"
const getURL = host + "/get"
const appendURL = host + "/append"

func check(t *testing.T, expected string, actual string) {
	if actual != expected {
		t.Fatalf("expected: %v, actual: %v", expected, actual)
	}
}

func Test1(t *testing.T) {
	if err := util.Post(joinURL, map[int]int{100: 3, 101: 3}, nil); err != nil {
		t.Fatal(err)
	}
	key := "1"
	value := "adsds"
	if err := util.Get(putURL, map[string]string{"key": key, "value": value}, nil); err != nil {
		t.Fatal(err)
	}

	var actual string
	if err := util.Get(getURL, map[string]string{"key": key}, &actual); err != nil {
		t.Fatal(err)
	}
	check(t, value, actual)

	appendValue := "22222"
	value += appendValue
	if err := util.Get(appendURL, map[string]string{"key": key, "value": appendValue}, nil); err != nil {
		t.Fatal(err)
	}
	if err := util.Get(getURL, map[string]string{"key": key}, &actual); err != nil {
		t.Fatal(err)
	}
	check(t, value, actual)

	if err := util.Post(leaveURL, []int{101}, nil); err != nil {
		t.Fatal(err)
	}
	if err := util.Get(getURL, map[string]string{"key": key}, &actual); err != nil {
		t.Fatal(err)
	}
	check(t, value, actual)
}