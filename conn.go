package gorqlite

/*
	this file contains some high-level Connection-oriented stuff
*/

/* *****************************************************************

   imports

 * *****************************************************************/

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	nurl "net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultTimeout                 = 2
	defaultDisableClusterDiscovery = false
)

var (
	// ErrClosed indicates that client connection was closed
	ErrClosed = errors.New("gorqlite: connection is closed")
	traceOut  io.Writer
)

// defaults to false.  This is used in trace() to quickly
// return if tracing is off, so that we don't do a perhaps
// expensive Sprintf() call only to send it to Discard
var wantsTrace bool

/* *****************************************************************

   type: Connection

 * *****************************************************************/

// Connection provides the connection abstraction.
// Note that since rqlite is stateless, there really is no "connection".
// However, this type holds  information such as the current leader, peers,
// connection string to build URLs, etc.
//
// Connections are assigned a "connection ID" which is a pseudo-UUID
// for connection identification in trace output only.  This helps
// sort out what's going on if you have multiple connections going
// at once.  It's generated using a non-standards-or-anything-else-compliant
// function that uses crypto/rand to generate 16 random bytes.
//
// Note that the Connection objection holds info on all peers, gathered
// at time of Open() from the node specified.
type Connection struct {
	cluster      rqliteCluster
	useStatusApi bool

	// name           type                default

	username                string           //   username or ""
	password                string           //   username or ""
	consistencyLevel        consistencyLevel //   WEAK
	disableClusterDiscovery bool             //   false unless user states otherwise
	wantsHTTPS              bool             //   false unless connection URL is https
	wantsTransactions       bool             //   true unless user states otherwise
	wantsQueueing           bool             //   perform queued writes

	// variables below this line need to be initialized in Open()
	timeout       int          //   2
	hasBeenClosed bool         //   false
	ID            string       //   generated in init()
	client        *http.Client //   user provided or nil
}

// Close will mark the connection as closed. It is safe to be called
// multiple times.
func (conn *Connection) Close() {
	conn.hasBeenClosed = true
	trace("%s: %s", conn.ID, "closing connection")
}

func (conn *Connection) apiClient(get bool) *http.Client {
	ret := conn.client
	if ret == nil {
		ret = &http.Client{}
		if get {
			ret.Timeout = time.Duration(conn.timeout) * time.Second
		}
	}
	return ret
}

// ConsistencyLevel tells the current consistency level
func (conn *Connection) ConsistencyLevel() (string, error) {
	if conn.hasBeenClosed {
		return "", ErrClosed
	}
	return consistencyLevelNames[conn.consistencyLevel], nil
}

// Leader tells the current leader of the cluster
func (conn *Connection) Leader(ctx context.Context) (string, error) {
	if conn.hasBeenClosed {
		return "", ErrClosed
	}
	if conn.disableClusterDiscovery {
		return string(conn.cluster.leader), nil
	}
	trace("%s: Leader(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo(ctx)
	if err != nil {
		trace("%s: Leader() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return "", err
	} else {
		trace("%s: Leader(), updateClusterInfo() OK", conn.ID)
	}
	return string(conn.cluster.leader), nil
}

// Peers tells the current peers of the cluster
func (conn *Connection) Peers(ctx context.Context) ([]string, error) {
	if conn.hasBeenClosed {
		var ans []string
		return ans, ErrClosed
	}
	plist := make([]string, 0)

	if conn.disableClusterDiscovery {
		for _, p := range conn.cluster.peerList {
			plist = append(plist, string(p))
		}
		return plist, nil
	}

	trace("%s: Peers(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo(ctx)
	if err != nil {
		trace("%s: Peers() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return plist, err
	} else {
		trace("%s: Peers(), updateClusterInfo() OK", conn.ID)
	}
	if conn.cluster.leader != "" {
		plist = append(plist, string(conn.cluster.leader))
	}
	for _, p := range conn.cluster.otherPeers {
		plist = append(plist, string(p))
	}
	return plist, nil
}

func (conn *Connection) SetConsistencyLevel(levelDesired string) error {
	if conn.hasBeenClosed {
		return ErrClosed
	}
	_, ok := consistencyLevels[levelDesired]
	if ok {
		conn.consistencyLevel = consistencyLevels[levelDesired]
		return nil
	}
	return errors.New(fmt.Sprintf("unknown consistency level: %s", levelDesired))
}

func (conn *Connection) SetConsistency(levelDesired consistencyLevel) error {
	if conn.hasBeenClosed {
		return ErrClosed
	}

	if levelDesired < ConsistencyLevelNone || levelDesired > ConsistencyLevelStrong {
		return fmt.Errorf("unknown consistency level: %d", levelDesired)
	}

	conn.consistencyLevel = levelDesired
	return nil
}

func (conn *Connection) SetExecutionWithTransaction(state bool) error {
	if conn.hasBeenClosed {
		return ErrClosed
	}
	conn.wantsTransactions = state
	return nil
}

// initConnection takes the initial connection URL specified by
// the user, and parses it into a peer.  This peer is assumed to
// be the leader.  The next thing Open() does is updateClusterInfo()
// so the truth will be revealed soon enough.
//
// initConnection() does not talk to rqlite.  It only parses the
// connection URL and prepares the new connection for work.
//
// URL format:
//
//	http[s]://${USER}:${PASSWORD}@${HOSTNAME}:${PORT}/db?[OPTIONS]
//
// Examples:
//
//	https://mary:secret2@localhost:4001/db
//	https://mary:secret2@server1.example.com:4001/db?level=none
//	https://mary:secret2@server2.example.com:4001/db?level=weak
//	https://mary:secret2@localhost:2265/db?level=strong
//
// to use default connection to localhost:4001 with no auth:
//
//	http://
//	https://
//
// guaranteed map fields - will be set to "" if not specified
//
//	field name                  default if not specified
//
//	username                    ""
//	password                    ""
//	hostname                    "localhost"
//	port                        "4001"
//	consistencyLevel            "weak"
//	timeout                     "2"
func (conn *Connection) initConnection(url string) error {
	// do some sanity checks.  You know users.

	if len(url) < 7 {
		return errors.New("url specified is impossibly short")
	}

	if !strings.HasPrefix(url, "http") {
		return errors.New("url does not start with 'http'")
	}

	u, err := nurl.Parse(url)
	if err != nil {
		return err
	}
	trace("%s: net.url.Parse() OK", conn.ID)

	if u.Scheme == "https" {
		conn.wantsHTTPS = true
	}

	// specs say Username() is always populated even if empty
	if u.User == nil {
		conn.username = ""
		conn.password = ""
	} else {
		// guaranteed, but could be empty which is ok
		conn.username = u.User.Username()

		// not guaranteed, so test if set
		pass, isset := u.User.Password()
		if isset {
			conn.password = pass
		} else {
			conn.password = ""
		}
	}

	if u.Host == "" {
		conn.cluster.leader = "localhost:4001"
	} else {
		conn.cluster.leader = peer(u.Host)
	}
	conn.cluster.peerList = []peer{conn.cluster.leader}

	// defaults
	conn.consistencyLevel = ConsistencyLevelWeak
	conn.timeout = defaultTimeout
	conn.wantsTransactions = true
	conn.disableClusterDiscovery = defaultDisableClusterDiscovery

	if u.RawQuery != "" {

		q := u.Query()
		level := q.Get("level")
		if level != "" {
			cl, ok := consistencyLevels[level]
			if !ok {
				return errors.New("invalid consistency level: " + level)
			}
			conn.consistencyLevel = cl
		}

		// timeout: default is set before initConnection is called
		ts := q.Get("timeout")
		if len(ts) > 0 {
			var ti int
			if ti, err = strconv.Atoi(ts); err != nil {
				return errors.New("invalid timeout: " + ts)
			}
			conn.timeout = ti
		}

		dcd := q.Get("disableClusterDiscovery")
		if dcd != "" {
			dpd, err := strconv.ParseBool(dcd)
			if err != nil {
				return errors.New("invalid disableClusterDiscovery value: " + err.Error())
			}
			conn.disableClusterDiscovery = dpd
		}
	}

	trace("%s: parseDefaultPeer() is done:", conn.ID)
	if conn.wantsHTTPS {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "yes")
	} else {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "no")
	}
	trace("%s:    %s -> %s", conn.ID, "username", conn.username)
	trace("%s:    %s -> %s", conn.ID, "password", conn.password)
	trace("%s:    %s -> %s", conn.ID, "host", conn.cluster.leader)
	trace("%s:    %s -> %s", conn.ID, "consistencyLevel", consistencyLevelNames[conn.consistencyLevel])
	trace("%s:    %s -> %v", conn.ID, "wantsTransaction", conn.wantsTransactions)
	trace("%s:    %s -> %v", conn.ID, "timeout", conn.timeout)
	trace("%s:    %s -> %v", conn.ID, "clusterDiscovery", !conn.disableClusterDiscovery)

	conn.cluster.conn = conn

	return nil
}
