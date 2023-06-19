package gorqlite

/*
	this file holds most of the cluster-related stuff:

	types:
		peer
		rqliteCluster
	Connection methods:
		assembleURL (from a peer)
		updateClusterInfo (does the full cluster discovery via status)
*/

/* *****************************************************************

   imports

 * *****************************************************************/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// peer is an internal type to abstract peer info, actually just
// represent a single hostname:port
type peer string

// internal type that abstracts the full cluster state (leader, peers)
type rqliteCluster struct {
	leader     peer
	otherPeers []peer
	peerList   []peer // cached list of peers starting with leader
	conn       *Connection
}

/* *****************************************************************

  method: rqliteCluster.makePeerList()

	in the api calls, we'll want to try the leader first, then the other
	peers.  to make looping easy, this function returns a list of peers
	in the order the try them: leader, other peer, other peer, etc.

 * *****************************************************************/

func (rc *rqliteCluster) makePeerList() []peer {
	trace("%s: makePeerList() called", rc.conn.ID)
	var peerList []peer
	if rc.leader != "" {
		peerList = append(peerList, rc.leader)
	}
	for _, p := range rc.otherPeers {
		peerList = append(peerList, p)
	}

	trace("%s: makePeerList() returning this list:", rc.conn.ID)
	for n, v := range peerList {
		trace("%s: makePeerList() peer %d -> %s", rc.conn.ID, n, v)
	}

	return peerList
}

// PeerList lists the peers within a rqlite cluster.
//
// In the api calls, we'll want to try the leader first, then the other
// peers. To make looping easy, this function returns a list of peers
// in the order the try them: leader, other peer, other peer, etc.
// Since the peer list might change only during updateClusterInfo(),
// we keep it cached
func (rc *rqliteCluster) PeerList() []peer {
	return rc.peerList
}

// tell it what peer to talk to and what kind of API operation you're
// making, and it will return the full URL, from start to finish.
// e.g.:
//
// https://mary:secret2@server1.example.com:1234/db/query?transaction&level=strong
//
// note: this func needs to live at the Connection level because the
// Connection holds the username, password, consistencyLevel, etc.
func (conn *Connection) assembleURL(apiOp apiOperation, p peer) string {
	var builder strings.Builder

	if conn.wantsHTTPS {
		builder.WriteString("https")
	} else {
		builder.WriteString("http")
	}
	builder.WriteString("://")
	if conn.username != "" && conn.password != "" {
		builder.WriteString(conn.username)
		builder.WriteString(":")
		builder.WriteString(conn.password)
		builder.WriteString("@")
	}
	builder.WriteString(string(p))

	switch apiOp {
	case api_STATUS:
		builder.WriteString("/status")
	case api_NODES:
		builder.WriteString("/nodes")
	case api_QUERY:
		builder.WriteString("/db/query")
	case api_WRITE:
		builder.WriteString("/db/execute")
	case api_REQUEST:
		builder.WriteString("/db/request")
	}

	if apiOp == api_QUERY || apiOp == api_WRITE || apiOp == api_REQUEST {
		builder.WriteString("?timings&level=")
		builder.WriteString(consistencyLevelNames[conn.consistencyLevel])
		if conn.wantsTransactions {
			builder.WriteString("&transaction")
		}
		if apiOp == api_WRITE && conn.wantsQueueing {
			builder.WriteString("&queue")
		}
	}

	switch apiOp {
	case api_QUERY:
		trace("%s: assembled URL for an api_QUERY: %s", conn.ID, builder.String())
	case api_STATUS:
		trace("%s: assembled URL for an api_STATUS: %s", conn.ID, builder.String())
	case api_NODES:
		trace("%s: assembled URL for an api_NODES: %s", conn.ID, builder.String())
	case api_WRITE:
		trace("%s: assembled URL for an api_WRITE: %s", conn.ID, builder.String())
	case api_REQUEST:
		trace("%s: assembled URL for an api_REQUEST: %s", conn.ID, builder.String())
	}

	return builder.String()
}

// Upon invocation, updateClusterInfo() completely erases and refreshes
// the Connection's cluster info, replacing its rqliteCluster object
// with current info.
//
// The web heavy lifting (retrying, etc.) is done in rqliteApiGet()
func (conn *Connection) updateClusterInfo(ctx context.Context) error {
	trace("%s: updateClusterInfo() called", conn.ID)

	// start with a fresh new cluster
	var rc rqliteCluster
	rc.conn = conn

	if conn.useStatusApi {
		// nodes/ API is available in 6.0+
		trace("getting leader from /status")
		responseBody, err := conn.rqliteApiGet(ctx, api_STATUS)
		if err != nil {
			// return errors.New("could not determine leader from API nodes call")
			return fmt.Errorf("cluster-info: could not determine leader from API nodes call: %v", err.Error())
		}
		trace("%s: updateClusterInfo() back from api call OK", conn.ID)

		sections := make(map[string]interface{})
		err = json.Unmarshal(responseBody, &sections)
		if err != nil {
			return err
		}
		sMap := sections["store"].(map[string]interface{})
		leaderMap, ok := sMap["leader"].(map[string]interface{})
		var leaderRaftAddr string
		if ok {
			leaderRaftAddr = leaderMap["node_id"].(string)
		} else {
			leaderRaftAddr = sMap["leader"].(string)
		}
		trace("%s: leader from store section is %s", conn.ID, leaderRaftAddr)

		// In 5.x and earlier, "metadata" is available
		// leader in this case is the RAFT address
		// we want the HTTP address, so we'll use this as
		// a key as we sift through APIPeers
		apiPeers, ok := sMap["metadata"].(map[string]interface{})
		if !ok {
			apiPeers = map[string]interface{}{}
		}

		if apiAddrMap, ok := apiPeers[leaderRaftAddr]; ok {
			if _httpAddr, ok := apiAddrMap.(map[string]interface{}); ok {
				if peerHttp, ok := _httpAddr["api_addr"]; ok {
					rc.leader = peer(peerHttp.(string))
				}
			}
		}
	}

	if rc.leader == "" {
		// nodes/ API is available in 6.0+
		if conn.useStatusApi {
			trace("getting leader from metadata failed, trying nodes/")
		} else {
			trace("getting leader from nodes/")
		}
		responseBody, err := conn.rqliteApiGet(ctx, api_NODES)
		if err != nil {
			return errors.New("cluster-info/no leader: could not determine leader from API nodes call")
		}
		trace("%s: updateClusterInfo() back from api call OK", conn.ID)

		// same as conn.processNodeInfoBody
		nodes := make(map[string]struct {
			APIAddr   string `json:"api_addr,omitempty"`
			Addr      string `json:"addr,omitempty"`
			Reachable bool   `json:"reachable,omitempty"`
			Leader    bool   `json:"leader"`
		})
		err = json.Unmarshal(responseBody, &nodes)
		if err != nil {
			return errors.New("could not unmarshal nodes/ response")
		}

		for _, v := range nodes {
			// dead peers are not reachable or have no http addr
			if !v.Reachable || v.APIAddr == "" {
				continue
			}

			u, err := url.Parse(v.APIAddr)
			if err != nil {
				return errors.New("could not parse API address")
			}
			trace("/nodes indicates %s as API Addr", u.String())

			if v.Leader {
				rc.leader = peer(u.Host)
			} else {
				rc.otherPeers = append(rc.otherPeers, peer(u.Host))
			}
		}
	} else {
		trace("leader successfully determined using metadata")
	}

	rc.peerList = []peer{}
	if rc.leader != "" {
		rc.peerList = append(rc.peerList, rc.leader)
	}

	rc.peerList = append(rc.peerList, rc.otherPeers...)

	// dump to trace
	trace("%s: here is my cluster config:", conn.ID)
	trace("%s: leader   : %s", conn.ID, rc.leader)
	for n, v := range rc.otherPeers {
		trace("%s: otherPeer #%d: %s", conn.ID, n, v)
	}

	// now make it official
	conn.cluster = rc

	return nil
}

/* *****************************************************************

	method: Connection.processNodeInfoBody()

	processes /nodes response from cluster, setting the leader and
	peers info, skipping unreachable peers

 * *****************************************************************/

func (conn *Connection) processNodeInfoBody(responseBody []byte, rc *rqliteCluster) (err error) {
	nodes := make(map[string]struct {
		APIAddr   string `json:"api_addr,omitempty"`
		Addr      string `json:"addr,omitempty"`
		Reachable bool   `json:"reachable,omitempty"`
		Leader    bool   `json:"leader"`
	})
	err = json.Unmarshal(responseBody, &nodes)
	if err != nil {
		return errors.New("could not unmarshal /nodes response")
	}

	var peers []peer
	for _, v := range nodes {
		// dead peers are not reachable or have no http addr
		if !v.Reachable || v.APIAddr == "" {
			continue
		}

		u, err := url.Parse(v.APIAddr)
		if err != nil {
			return errors.New("could not parse API address")
		}
		trace("/nodes indicates %s as API Addr", u.String())
		var p peer
		if _, _, err = net.SplitHostPort(u.Host); err != nil {
			p = peer(fmt.Sprintf("%s:%d", u.Host, 4001))
		} else {
			p = peer(u.Host)
		}
		if v.Leader {
			rc.leader = p
		} else {
			peers = append(peers, p)
		}
	}
	rc.otherPeers = peers

	return
}
