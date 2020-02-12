package core

import (
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/net/context"
	"math"
	"time"
)

// Raft tutorial
// - An attempt to understand etcd's RAFT lib.
// References
// 1) https://www.reddit.com/r/golang/comments/3dzprs/using_the_etcd_raft_to_build_your_first_cluster/
// 2) https://otm.github.io/2015/05/raft-a-first-implementation/
// 3) https://github.com/otm/raft-part-1/blob/3ec341756c99d0f866a1e39099ae780fb662ab37/raft-part-1.go
// 4) https://godoc.org/github.com/coreos/etcd/raft
type RaftyNode struct {
	// Raft unique id of this node
	id uint64

	// Underlying raft.Node
	node raft.Node

	// Underlying store
	store *raft.MemoryStorage

	// context required to send messages
	ctx context.Context

	// signals proposal channel closed
	stopc chan struct{}

	// references the raft network  this node is associated with
	raftNet *RaftNet

	// commit channel (all entries that need to be committed are sent here
	commitC chan<- *string

	// propose channel
	proposeC <-chan string
}

const heartBeatInterval = 1
const electionTimeoutInterval = 10 * heartBeatInterval

func NewRaftyNode(id uint64, peerIds []uint64, raftNet *RaftNet, commitC chan<- *string, proposeC <-chan string) *RaftyNode {
	storage := raft.NewMemoryStorage()
	cfg := &raft.Config{
		ID:              id,
		ElectionTick:    electionTimeoutInterval,
		HeartbeatTick:   heartBeatInterval,
		Storage:         storage,
		MaxSizePerMsg:   math.MaxUint32,
		MaxInflightMsgs: 256,
		Logger:          log.New(),
	}

	peers := make([]raft.Peer, 0, len(peerIds))
	for _, peerId := range peerIds {
		peers = append(peers, raft.Peer{
			ID: peerId,
		})
	}

	n := raft.StartNode(cfg, peers)
	log.Infof("started node %v", n)
	return &RaftyNode{
		id:       id,
		node:     n,
		store:    storage,
		raftNet:  raftNet,
		ctx:      context.TODO(),
		commitC:  commitC,
		proposeC: proposeC,
	}
}

func (rn *RaftyNode) Run() {
	go func() {
		// support cases where the nodes does not pick up proposals
		for rn.proposeC != nil {
			select {
			case prop, ok := <-rn.proposeC:
				if !ok {
					log.Infof("proposeC to nil")
					rn.proposeC = nil
				} else {
					err := rn.node.Propose(rn.ctx, []byte(prop))
					if err != nil {
						log.Panicf("rn.node.Propose(..) err=%v", err)
					}
				}
			}
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// Call Node.Tick() at regular intervals. Internally the raft time
			// is represented by an abstract tick, that controls two important
			// timeouts, the heartbeat and election timeout.
			rn.node.Tick()

		case rd := <-rn.node.Ready():
			log.Infof("ready recv %v", rd)

			// Write HardState, Entries and Snapshot to persistent storage. Note!
			// When writing to storage it is important to check the Entry Index (i).
			// If previously persisted entries with Index >= i exist, those entries
			// needs to be discarded. For instance this can happen if we get a cluster
			// split with the leader in the minority part; because then the cluster
			// can advance in the other part.
			// ToDo: Unsure if the entryIndex i is acutally checked!
			rn.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)

			// Send all messages to the nodes named in the To field. Note! It is important
			// to not send any messages until:
			// - the latest HardState has been persisted
			// - all Entries from previous batch have been persisted (messages from the current
			//   batch can be sent and persisted in parallel)
			// - call Node.ReportSnapshot() if any message has type MsgSnap and the snapshot
			//   has been sent
			rn.send(rd.Messages)

			// Apply Snapshot and CommitedEntries to the state machine. If any committed Entry
			// has the type EntryConfChange call Node.ApplyConfChange to actually apply it to
			// the node. The configuration change can be canceled at this point by setting the
			// NodeId field to zero before calling ApplyConfChange. Either way, ApplyConfChange
			// must be called; and the decision to cancel must be based solely on the state machine
			// and not on external information, for instance observed health state of a node.
			if !raft.IsEmptySnap(rd.Snapshot) {
				// ToDo; figure out the difference between a snapshot & committed Entries
				rn.processSnapshot(rd.Snapshot)
			}

			for _, entry := range rd.CommittedEntries {
				rn.process(entry)
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					err := cc.Unmarshal(entry.Data)
					if err != nil {
						log.Panic("cc.Unmarshal failed err=%v", err)
					}

					rn.node.ApplyConfChange(cc)
				}
			}

			// Call Node.Advance() to signal readiness for the next batch. This can be done any time
			// after step 1 is finished. Note! All updates must be processed in the order they
			// were received by Node.Ready()
			rn.node.Advance()

		case <-rn.stopc:
			rn.stop()
			return
		}
	}
}

func (rn *RaftyNode) saveToStorage(hardState raftpb.HardState,
	entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	err := rn.store.Append(entries)
	if err != nil {
		log.Panicf("rn.store.Append error=%v", err)
	}

	if !raft.IsEmptyHardState(hardState) {
		err := rn.store.SetHardState(hardState)
		if err != nil {
			log.Panicf("rn.store.SetHardState error=%v", err)
		}
	}

	if !raft.IsEmptySnap(snapshot) {
		err := rn.store.ApplySnapshot(snapshot)
		if err != nil {
			log.Panicf("rn.store.ApplySnapshot err=%v", err)
		}
	}
}

func (rn *RaftyNode) send(messages []raftpb.Message) {
	for _, m := range messages {
		log.Println(raft.DescribeMessage(m, nil))
		// send a message with current context and the specific node
		rn.raftNet.Send(rn.ctx, m.To, m)
	}
}

func (rn *RaftyNode) recvRaftRPC(ctx context.Context, m raftpb.Message) {
	rn.node.Step(ctx, m)
}

func (rn *RaftyNode) processSnapshot(snapshot raftpb.Snapshot) {
	log.Panicf("Applying snapshot on node: %v is not implemented", rn.id)
}

func (n *RaftyNode) process(entry raftpb.Entry) {
	log.Infof("node %v: processing entry: %v Type: %v", n.id, entry, entry.Type)
	if entry.Type == raftpb.EntryNormal && entry.Data != nil {
		// ToDo: Don't see a point in making a str (is there an advantage to
		// sending a slice across the channel
		s := string(entry.Data)
		n.commitC <- &s
	} else {
		log.Warnf("Skipping process entry")
	}
}

// stop closes http, closes all channels, and stops raft.
func (rn *RaftyNode) stop() {
	rn.node.Stop()
}
