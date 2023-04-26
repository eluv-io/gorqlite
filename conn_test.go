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
	if conn, err = parseUrl("https://user1:pass1@host1:4000/db?level=strong&timeout=1"); err != nil {
		t.Error(err)
	}

	requireBool(t, true, conn.wantsHTTPS)
	requireBool(t, true, conn.wantsTransactions) // will always be true until settable
	requireString(t, "user1", conn.username)
	requireString(t, "pass1", conn.password)
	requireString(t, "host1:4000", string(conn.cluster.leader))
	requireString(t, "strong", consistencyLevelNames[conn.consistencyLevel])
	requireInt(t, 1, conn.timeout)

	//goland:noinspection HttpUrlsUsage
	if conn, err = parseUrl("http://user1:pass1@host1.foobar.com:4000/db?level=weak"); err != nil {
		t.Error(err)
	}

	requireBool(t, false, conn.wantsHTTPS)
	requireBool(t, true, conn.wantsTransactions) // will always be true until settable
	requireString(t, "user1", conn.username)
	requireString(t, "pass1", conn.password)
	requireString(t, "host1.foobar.com:4000", string(conn.cluster.leader))
	requireString(t, "weak", consistencyLevelNames[conn.consistencyLevel])
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

func TestSetConsistencyLevel(t *testing.T) {
	conn, err := Open(testUrl())
	if err != nil {
		t.Fatalf("failed to open connection: %v", err.Error())
	}

	t.Run("Less than none", func(t *testing.T) {
		err := conn.SetConsistencyLevel(-1)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("Greater than strong", func(t *testing.T) {
		err := conn.SetConsistencyLevel(100)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	t.Run("None", func(t *testing.T) {
		err := conn.SetConsistencyLevel(ConsistencyLevelNone)
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}

		currentLevel, err := conn.ConsistencyLevel()
		if err != nil {
			t.Errorf("unexpected error: %s", err.Error())
		}

		if currentLevel != "none" {
			t.Errorf("expected currentLevel to be 'none', instead got %s", currentLevel)
		}
	})

	conn.Close()
}

func TestSetExecutionWithTransaction(t *testing.T) {
	conn, err := Open(testUrl())
	if err != nil {
		t.Fatalf("failed to open connection: %v", err.Error())
	}

	err = conn.SetExecutionWithTransaction(true)
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}

	conn.Close()
}
