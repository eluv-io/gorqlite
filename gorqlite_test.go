package gorqlite

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"
)

var globalConnection *Connection

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())

	conn, err := Open(testUrl())
	if err != nil {
		log.Fatalf("opening connection: %v", err)
	}

	err = conn.SetConsistency(ConsistencyLevelStrong)
	if err != nil {
		log.Fatalf("setting consistency level: %v", err)
	}

	globalConnection = conn

	exitCode := m.Run()

	conn.Close()

	os.Exit(exitCode)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func testUrl() string {
	url := os.Getenv("GORQLITE_TEST_URL")
	if url == "" {
		url = "http://localhost:4001"
	}
	return url
}

func testUrlMultiple() string {
	url := os.Getenv("GORQLITE_TEST_URLS")
	if url == "" {
		url = "http://1.2.3.4:1234,http://localhost:4001"
	}
	return url
}

func testTableName() string {
	tableName := os.Getenv("GORQLITE_TEST_TABLE")
	if tableName == "" {
		tableName = "gorqlite_test"
	}
	return tableName
}
