package gorqlite

import (
	"context"
	"testing"
)

func TestProcessInfoResponse(t *testing.T) {
	testNodeInfoResponse := `{
  "1": {
    "api_addr": "http://host1:4001",
    "addr": "host2:4002",
    "reachable": true,
    "leader": false,
    "time": 9.114e-06
  },
  "2": {
    "api_addr": "http://host3:4003",
    "addr": "host3:4004",
    "reachable": true,
    "leader": true,
    "time": 0.000127793
  },
  "3": {
    "addr": "host6:4006",
    "reachable": false,
    "leader": false,
    "error": "pool get: dial tcp host6:4006: connect: connection refused"
  }
}
`
	testConn := Connection{}
	var rc rqliteCluster
	err := testConn.processNodeInfoBody([]byte(testNodeInfoResponse), &rc)
	if err != nil || len(rc.otherPeers) == 0 {
		t.Fatal(err)
	}
	if rc.leader != "host3:4003" {
		t.Errorf("leader should be host3:4003, got %s", rc.leader)
	}
	if len(rc.otherPeers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(rc.otherPeers))
	}
	p := rc.otherPeers[0]
	if p != "host1:4001" {
		t.Errorf("peer should be host1:4001, got %s", p)
	}
}

func TestInitCluster(t *testing.T) {

	//TraceOn(os.Stderr)
	t.Logf("trying Open: %s\n", testUrl())
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FAILED")
		t.Fatal(err)
	}

	l, err := conn.Leader(context.Background())
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(l) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	p, err := conn.Peers(context.Background())
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(p) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

}

func TestLeader(t *testing.T) {
	leaders, err := globalConnection.Leader(context.Background())
	if err != nil {
		t.Errorf("failed to get leader: %v", err.Error())
	}

	if len(leaders) < 1 {
		t.Errorf("expected leaders to be at least 1, got %d", len(leaders))
	}
}

func TestPeers(t *testing.T) {
	peers, err := globalConnection.Peers(context.Background())
	if err != nil {
		t.Errorf("failed to get peers: %v", err.Error())
	}

	if len(peers) < 1 {
		t.Errorf("expected peers to be at least 1, got %d", len(peers))
	}
}
