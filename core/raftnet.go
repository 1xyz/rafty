package core

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
	"time"
)

type raftNodeMap map[uint64]*RaftyKVNode

type RaftyKVNode struct {
	// Reference to the raft node
	raftyNode *RaftyNode

	// Reference to a key store at that node
	kvStore *KVStore

	// snap shot directory
	snapDir string
}

func NewRaftyKVNode(nodeID uint64, peerIDs []uint64, raftNet *RaftNet) *RaftyKVNode {
	proposeC := make(chan string)
	// confChangeC := make(chan raftpb.ConfChange)
	commitC := make(chan *string)
	errorC := make(chan error)
	snapDir := fmt.Sprintf("/tmp/rafty/node-%d", nodeID)
	if !fileutil.Exist(snapDir) {
		if err := os.Mkdir(snapDir, 0750); err != nil {
			log.Panicf("os.mkDir: cannot create dir for snapshot (%v)", err)
		}
	}

	snapShotter := snap.New(zap.NewExample(), snapDir)
	return &RaftyKVNode{
		raftyNode: NewRaftyNode(nodeID, peerIDs, raftNet, commitC, proposeC),
		kvStore:   NewKVStore(snapShotter, proposeC, commitC, errorC),
		snapDir:   snapDir,
	}
}

type RaftNet struct {
	nodes raftNodeMap
}

func NewRaftNet(nc uint64) *RaftNet {
	raftNet := &RaftNet{
		nodes: make(raftNodeMap),
	}

	peerIds := make([]uint64, 0, nc)
	for i := 0; uint64(i) < nc; i++ {
		peerIds = append(peerIds, uint64(i)+1)
	}

	for _, id := range peerIds {
		raftNet.nodes[id] = NewRaftyKVNode(id, peerIds, raftNet)
	}

	return raftNet
}

func (rNet *RaftNet) Run() {
	// campaign the first node, basically force node 1 to switch
	// from follower to candidate, to try to become a leader
	n1 := rNet.nodes[1].raftyNode
	log.Infof("Attempt to switch node %v to campaign mode", n1.id)
	err := n1.node.Campaign(n1.ctx)
	if err != nil {
		log.Panicf("node.Campaign error=%v", err)
	}

	log.Infof("Node length = %v", len(rNet.nodes))
	log.Infof("Attempt to start %v nodes", len(rNet.nodes))
	for id, n := range rNet.nodes {
		log.Infof("run node %v", id)
		go n.raftyNode.Run()
	}

	n2 := rNet.nodes[2].raftyNode
	log.Infof("Attempt a conf change to add node 3")
	err = n2.node.ProposeConfChange(n2.ctx, raftpb.ConfChange{
		ID:      3,
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  3,
		Context: []byte(""),
	})
	if err != nil {
		log.Panicf("n2.node.ProposeConfChange err=%v", err)
	}

	for i := 0; i < 2; i++ {
		if n1.node.Status().Lead == 1 {
			break
		}

		log.Infof("Waiting for a node %v to become a leader current=%v", n1.id, n1.node.Status().Lead)
		time.Sleep(2 * time.Second)
	}

	log.Infof("raftNet run/init complete")
}

func (rNet *RaftNet) ProposeChange(srcID uint64, key, value string) {
	src, ok := rNet.nodes[srcID]
	if !ok {
		log.Panicf("cannot find node with id=%v", srcID)
	}

	defer log.Infof("Propose key=%v value=%v at node=%v",
		key, value, src.raftyNode.id)
	src.kvStore.Propose(key, value)
}

func (rNet *RaftNet) ReadFromAllNodes(key string) map[uint64]string {
	result := make(map[uint64]string)
	for nodeID, raftyKVNode := range rNet.nodes {
		v, ok := raftyKVNode.kvStore.Lookup(key)
		if ok {
			result[nodeID] = v
		} else {
			result[nodeID] = "Not-found"
		}
	}

	return result
}

func (rNet *RaftNet) Send(ctx context.Context, id uint64, m raftpb.Message) {
	rNet.nodes[id].raftyNode.recvRaftRPC(ctx, m)
}
