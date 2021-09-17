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
	"net"
	"net/http"
	nurl "net/url"
	"strconv"
	"strings"
	"time"
)

var errClosed = errors.New("gorqlite: connection is closed")
var traceOut io.Writer

// defaults to false.  This is used in trace() to quickly
// return if tracing is off, so that we don't do a perhaps
// expensive Sprintf() call only to send it to Discard

var wantsTrace bool

/* *****************************************************************

   type: Connection

 * *****************************************************************/

/*
	The connection abstraction.	 Note that since rqlite is stateless,
	there really is no "connection".  However, this type holds
	information such as the current leader, peers, connection
	string to build URLs, etc.

	Connections are assigned a "connection ID" which is a pseudo-UUID
	for connection identification in trace output only.  This helps
	sort out what's going on if you have multiple connections going
	at once.  It's generated using a non-standards-or-anything-else-compliant
	function that uses crypto/rand to generate 16 random bytes.

	Note that the Connection objection holds info on all peers, gathered
	at time of Open() from the node specified.
*/

type Connection struct {
	cluster rqliteCluster

	/*
	  name               type                default
	*/

	username          string           //   username or ""
	password          string           //   username or ""
	consistencyLevel  consistencyLevel //   WEAK
	wantsHTTPS        bool             //   false unless connection URL is https
	wantsTransactions bool             //   true unless user states otherwise

	// variables below this line need to be initialized in Open()

	timeout       int          //   2
	hasBeenClosed bool         //   false
	ID            string       //   generated in init()
	client        *http.Client //   user provided or nil
}

var defaultTimeout = 2

/* *****************************************************************

   method: Connection.Close()

 * *****************************************************************/

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

/* *****************************************************************

   method: Connection.ConsistencyLevel()

 * *****************************************************************/

func (conn *Connection) ConsistencyLevel() (string, error) {
	if conn.hasBeenClosed {
		return "", errClosed
	}
	return consistencyLevelNames[conn.consistencyLevel], nil
}

/* *****************************************************************

   method: Connection.Leader()

 * *****************************************************************/

func (conn *Connection) Leader(ctx context.Context) (string, error) {
	if conn.hasBeenClosed {
		return "", errClosed
	}
	trace("%s: Leader(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo(ctx)
	if err != nil {
		trace("%s: Leader() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return "", err
	} else {
		trace("%s: Leader(), updateClusterInfo() OK", conn.ID)
	}
	return conn.cluster.leader.String(), nil
}

/* *****************************************************************

   method: Connection.Peers()

 * *****************************************************************/

func (conn *Connection) Peers(ctx context.Context) ([]string, error) {
	if conn.hasBeenClosed {
		var ans []string
		return ans, errClosed
	}
	plist := make([]string, 0)

	trace("%s: Peers(), calling updateClusterInfo()", conn.ID)
	err := conn.updateClusterInfo(ctx)
	if err != nil {
		trace("%s: Peers() got error from updateClusterInfo(): %s", conn.ID, err.Error())
		return plist, err
	} else {
		trace("%s: Peers(), updateClusterInfo() OK", conn.ID)
	}
	plist = append(plist, conn.cluster.leader.String())
	for _, p := range conn.cluster.otherPeers {
		plist = append(plist, p.String())
	}
	return plist, nil
}

/* *****************************************************************

   method: Connection.SetConsistencyLevel()

 * *****************************************************************/

func (conn *Connection) SetConsistencyLevel(levelDesired string) error {
	if conn.hasBeenClosed {
		return errClosed
	}
	_, ok := consistencyLevels[levelDesired]
	if ok {
		conn.consistencyLevel = consistencyLevels[levelDesired]
		return nil
	}
	return errors.New(fmt.Sprintf("unknown consistency level: %s", levelDesired))
}

func (conn *Connection) SetExecutionWithTransaction(state bool) error {
	if conn.hasBeenClosed {
		return errClosed
	}
	conn.wantsTransactions = state
	return nil
}

/* *****************************************************************

   method: Connection.initConnection()

 * *****************************************************************/

/*
	initConnection takes the initial connection URL specified by
	the user, and parses it into a peer.  This peer is assumed to
	be the leader.  The next thing Open() does is updateClusterInfo()
	so the truth will be revealed soon enough.

	initConnection() does not talk to rqlite.  It only parses the
	connection URL and prepares the new connection for work.

	URL format:

		http[s]://${USER}:${PASSWORD}@${HOSTNAME}:${PORT}/db?[OPTIONS]

	Examples:

		https://mary:secret2@localhost:4001/db
		https://mary:secret2@server1.example.com:4001/db?level=none
		https://mary:secret2@server2.example.com:4001/db?level=weak
		https://mary:secret2@localhost:2265/db?level=strong?timeout=1

	to use default connection to localhost:4001 with no auth:
		http://
		https://

	guaranteed map fields - will be set to "" if not specified

		field name                  default if not specified

		username                    ""
		password                    ""
		hostname                    "localhost"
		port                        "4001"
		consistencyLevel            "weak"
		timeout						"2"
*/

func (conn *Connection) initConnection(url string) error {

	// do some sanity checks.  You know users.

	if len(url) < 7 {
		return errors.New("url specified is impossibly short")
	}

	if strings.HasPrefix(url, "http") == false {
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
		conn.cluster.leader.hostname = "localhost"
	} else {
		conn.cluster.leader.hostname = u.Host
	}

	if u.Host == "" {
		conn.cluster.leader.hostname = "localhost"
		conn.cluster.leader.port = "4001"
	} else {
		// SplitHostPort() should only return an error if there is no host port.
		// I think.
		h, p, err := net.SplitHostPort(u.Host)
		if err != nil {
			conn.cluster.leader.hostname = u.Host
		} else {
			conn.cluster.leader.hostname = h
			conn.cluster.leader.port = p
		}
	}

	// defaults
	conn.consistencyLevel = cl_WEAK
	conn.timeout = defaultTimeout
	conn.wantsTransactions = true

	if u.RawQuery != "" {
		q := u.Query()
		level := q.Get("level")
		switch level {
		case "", "weak":
		case "strong":
			conn.consistencyLevel = cl_STRONG
		case "none":
			conn.consistencyLevel = cl_NONE
		default:
			return errors.New("invalid level: " + level)
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
	}

	trace("%s: parseDefaultPeer() is done:", conn.ID)
	if conn.wantsHTTPS == true {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "yes")
	} else {
		trace("%s:    %s -> %s", conn.ID, "wants https?", "no")
	}
	trace("%s:    %s -> %s", conn.ID, "username", conn.username)
	trace("%s:    %s -> %s", conn.ID, "password", conn.password)
	trace("%s:    %s -> %s", conn.ID, "hostname", conn.cluster.leader.hostname)
	trace("%s:    %s -> %s", conn.ID, "port", conn.cluster.leader.port)
	trace("%s:    %s -> %s", conn.ID, "consistencyLevel", consistencyLevelNames[conn.consistencyLevel])
	trace("%s:    %s -> %v", conn.ID, "wantTransaction", conn.wantsTransactions)
	trace("%s:    %s -> %v", conn.ID, "timeout", conn.timeout)

	conn.cluster.conn = conn

	return nil
}
