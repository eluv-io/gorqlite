package gorqlite

import (
	"errors"
	"testing"
)

func parseUrl(url string) (conn Connection, err error) {
	conn = Connection{}
	if err = conn.initConnection(url); err != nil {
		return
	}
	return
}

func TestConnectionStringParse(t *testing.T) {

	var err error
	if _, err = parseUrl(""); err == nil {
		t.Error(errors.New("should have got error for empty string URL"))
	}

	var conn Connection
	if conn, err = parseUrl("https://user1:pass1@host1:4000/db?level=strong&timeout=1"); err !=nil {
		t.Error(err)
	}

	requireBool(t, true, conn.wantsHTTPS)
	requireBool(t, true, conn.wantsTransactions) // will always be true until settable
	requireString(t, "user1", conn.username)
	requireString(t, "pass1", conn.password)
	requireString(t, "host1", conn.cluster.leader.hostname)
	requireString(t, "strong", consistencyLevelNames[conn.consistencyLevel])
	requireString(t, "4000", conn.cluster.leader.port)
	requireInt(t, 1, conn.timeout)

	if conn, err = parseUrl("http://user1:pass1@host1.foobar.com:4000/db?level=weak"); err !=nil {
		t.Error(err)
	}

	requireBool(t, false, conn.wantsHTTPS)
	requireBool(t, true, conn.wantsTransactions) // will always be true until settable
	requireString(t, "user1", conn.username)
	requireString(t, "pass1", conn.password)
	requireString(t, "host1.foobar.com", conn.cluster.leader.hostname)
	requireString(t, "weak", consistencyLevelNames[conn.consistencyLevel])
	requireString(t, "4000", conn.cluster.leader.port)
	requireInt(t, defaultTimeout, conn.timeout)
}

func requireString(t *testing.T, expected string, actual string) {
	if expected != actual {
		t.Errorf("expected '%v', got '%v'", expected, actual)
	}
}

func requireBool(t *testing.T, expected bool, actual bool) {
	if expected != actual {
		t.Errorf("expected '%v', got '%v'", expected, actual)
	}
}

func requireInt(t *testing.T, expected int, actual int) {
	if expected != actual {
		t.Errorf("expected '%v', got '%v'", expected, actual)
	}
}
