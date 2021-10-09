package gorqlite

import "testing"

import "os"

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
	if rc.leader.hostname != "host3" || rc.leader.port != "4003" {
		t.Errorf("leader should be host3:4003, got %s:%s", rc.leader.hostname, rc.leader.port)
	}
	if len(rc.otherPeers) != 1 {
		t.Errorf("expected 1 peer, got %d", len(rc.otherPeers))
	}
	p := rc.otherPeers[0]
	if p.hostname != "host1" || p.port != "4001" {
		t.Errorf("peer should be host1:4001, got %s:%s", p.hostname, p.port)
	}
}

func TestInitCluster(t *testing.T) {

	TraceOn(os.Stderr)
	t.Logf("trying Open: %s\n", testUrl())
	conn, err := Open(testUrl())
	if err != nil {
		t.Logf("--> FAILED")
		t.Fatal(err)
	}

	l, err := conn.Leader()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(l) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}

	p, err := conn.Peers()
	if err != nil {
		t.Logf("--> FAILED")
		t.Fail()
	}

	if len(p) < 1 {
		t.Logf("--> FAILED")
		t.Fail()
	}
}

func TestFavoredSeed(t *testing.T) {

	conn := Connection{ID: "testID"}

	seed := peer{hostname: "127.0.0.1", port: "4001"}
	leader := peer{hostname: "10.100.1.1", port: "4001"}

	cluster := rqliteCluster{
		conn:   &conn,
		leader: leader,
		seed:   seed,
		otherPeers: []peer{
			{hostname: "10.100.2.1", port: "4001"},
			{hostname: "10.100.3.1", port: "4001"},
			seed,
			{hostname: "10.100.4.1", port: "4001"},
		},
	}

	peerList := cluster.makePeerList(true)
	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == seed)
	requireBool(t, true, peerList[1] == leader)

	peerList = cluster.makePeerList(false)
	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == leader)
	requireBool(t, true, peerList[3] == seed)

	cluster = rqliteCluster{
		conn:   &conn,
		leader: leader,
		seed:   seed,
		otherPeers: []peer{
			{hostname: "10.100.2.1", port: "4001"},
			{hostname: "10.100.3.1", port: "4001"},
			seed,
			{hostname: "10.100.5.1", port: "4001"},
		},
	}

	peerList = cluster.makePeerList(true)
	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == seed)
	requireBool(t, true, peerList[1] == leader)

	peerList = cluster.makePeerList(false)

	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == leader)
	requireBool(t, true, peerList[3] == seed)

	other := leader
	cluster = rqliteCluster{
		conn:   &conn,
		leader: seed,
		seed:   seed,
		otherPeers: []peer{
			{hostname: "10.100.2.1", port: "4001"},
			{hostname: "10.100.3.1", port: "4001"},
			other,
			{hostname: "10.100.5.1", port: "4001"},
		},
	}

	peerList = cluster.makePeerList(true)
	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == seed)
	requireBool(t, true, peerList[3] == other)

	peerList = cluster.makePeerList(false)
	requireBool(t, true, len(peerList) == 5)
	requireBool(t, true, peerList[0] == seed)
	requireBool(t, true, peerList[3] == other)
}
