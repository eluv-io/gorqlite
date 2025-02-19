// Package gorqlite provieds a database/sql-like driver for rqlite,
// the distributed consistent sqlite.
//
// Copyright (c)2016 andrew fabbro (andrew@fabbro.org)
//
// See LICENSE.md for license. tl;dr: MIT. Conveniently, the same license as rqlite.
//
// Project home page: https://github.com/raindo308/gorqlite
//
// Learn more about rqlite at: https://github.com/rqlite/rqlite
package gorqlite

// this file contains package-level stuff:
//   consts
//   init()
//   Open, TraceOn(), TraceOff()

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type consistencyLevel int

const (
	// ConsistencyLevelNone provides no consistency to other nodes.
	ConsistencyLevelNone consistencyLevel = iota
	// ConsistencyLevelWeak provides a weak consistency that guarantees the
	// queries are sent to the leader.
	ConsistencyLevelWeak
	// ConsistencyLevelStrong provides a strong consistency and guarantees
	// that queries are sent and received by other nodes.
	ConsistencyLevelStrong
)

// used in several places, actually
var (
	consistencyLevelNames map[consistencyLevel]string
	consistencyLevels     map[string]consistencyLevel
)

type apiOperation int

const (
	api_QUERY apiOperation = iota
	api_STATUS
	api_WRITE
	api_NODES
	api_REQUEST
)

func init() {
	Discard = &DiscardTracer{}
	tracer = Discard

	consistencyLevelNames = make(map[consistencyLevel]string)
	consistencyLevelNames[ConsistencyLevelNone] = "none"
	consistencyLevelNames[ConsistencyLevelWeak] = "weak"
	consistencyLevelNames[ConsistencyLevelStrong] = "strong"

	consistencyLevels = make(map[string]consistencyLevel)
	consistencyLevels["none"] = ConsistencyLevelNone
	consistencyLevels["weak"] = ConsistencyLevelWeak
	consistencyLevels["strong"] = ConsistencyLevelStrong
}

// Open creates and returns a "connection" to rqlite.
//
// Since rqlite is stateless, there is no actual connection.
// Open creates and initializes a gorqlite Connection, which represents various
// config information.
//
// The URL is a comma-separated list of URLs to individual "seed" nodes of the cluster:
//
//	http://frank:mySecret@host1:4001?level=strong&timeout=2,http://host2:4444,http://host3
//
// The first node URL may have additional settings (username, password, level, timeout)
// and should be of the following form:
//
//	http://[username:password@]localhost[:4001][?level=none|weak|strong&timeout=1]
//
// Defaults:
//
//	port:     4001
//	username: empty
//	password: empty
//	level:    weak
//	timeout:  2 (seconds)
func Open(connURL string, client ...*http.Client) (*Connection, error) {
	return OpenContext(context.Background(), connURL, client...)
}

func OpenContext(ctx context.Context, connURL string, client ...*http.Client) (*Connection, error) {
	var cl *http.Client
	if len(client) > 0 {
		cl = client[0]
	}
	conn := &Connection{
		client: cl,
	}

	// generate our uuid for trace
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return conn, err
	}
	conn.ID = fmt.Sprintf("%X-%X-%X-%X-%X", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	trace("%s: Open() called for url: %s", conn.ID, connURL)

	// set defaults
	conn.hasBeenClosed = false

	urls := strings.Split(connURL, ",")

	// parse the first URL for full settings (including options)
	err = conn.initConnection(urls[0])
	if err != nil {
		return conn, err
	}

	// any subsequent URLs are used as additional peers
	for _, peerUrl := range urls[1:] {
		pu, err := url.Parse(peerUrl)
		if err != nil {
			return conn, err
		}
		var other peer
		_, _, err = net.SplitHostPort(pu.Host)
		if err != nil {
			other = peer(fmt.Sprintf("%s:%d", pu.Host, 4001))
		} else {
			other = peer(pu.Host)
		}
		conn.cluster.otherPeers = append(conn.cluster.otherPeers, other)
	}
	conn.cluster.peerList = append(conn.cluster.peerList, conn.cluster.otherPeers...)

	if !conn.disableClusterDiscovery {
		// call updateClusterInfo() to re-populate the cluster and discover peers
		// also tests the user's default
		if err := conn.updateClusterInfo(ctx); err != nil {
			return conn, err
		}
	}

	return conn, nil
}

// trace adds a message to the trace output
//
// not a public function.  we (inside) can add - outside they can
// only see.
//
// Call trace as:     Sprintf pattern , args...
//
// This is done so that the more expensive Sprintf() stuff is
// done only if truly needed.  When tracing is off, calls to
// trace() just hit a bool check and return.  If tracing is on,
// then the Sprintf-ing is done at a leisurely pace because, well,
// we're tracing.
//
// Premature optimization is the root of all evil, so this is
// probably sinful behavior.
//
// Don't put a \n in your Sprintf pattern because trace() adds one
func trace(pattern string, args ...interface{}) {
	// don't do the probably expensive Sprintf() if not needed
	tracer.Tracef(pattern, args...)
}

// TraceOn turns on tracing output to the io.Writer of your choice.
//
// Trace output is very detailed and verbose, as you might expect.
//
// Normally, you should run with tracing off, as it makes absolutely
// no concession to performance and is intended for debugging/dev use.
func TraceOn(w io.Writer) {
	tracer = &traceWriter{out: w}
}

// WithTracer turns on tracing output to the given Tracer.
func WithTracer(t Tracer) {
	tracer = t
}

// TraceOff turns off tracing output. Once you call TraceOff(), no further
// info is sent to the io.Writer, unless it is TraceOn'd again.
func TraceOff() {
	tracer = Discard
}

type Tracer interface {
	Tracef(pattern string, args ...interface{})
}

var Discard *DiscardTracer

type DiscardTracer struct{}

func (d *DiscardTracer) Tracef(string, ...interface{}) {}

type traceWriter struct {
	out io.Writer
}

func (t *traceWriter) Tracef(pattern string, args ...interface{}) {
	// make sure there is one and only one newline
	nlPattern := strings.TrimSpace(pattern) + "\n"
	msg := fmt.Sprintf(nlPattern, args...)
	_, _ = t.out.Write([]byte(msg))
}
