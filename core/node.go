package core

import (
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"math"
	"os"
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

	// Configuration state of this node (I think)!!!
	confState raftpb.ConfState

	// function to retrieve the store snapshot as a byte slice
	getSnapshot func() ([]byte, error)

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

	// reference to a write-ahead log
	wal *wal.WAL

	// path to dir, where the Write-ahead-log is stored
	waldir string

	// index of log at start (i.e when the WAL is loaded the first time)
	lastIndex uint64

	// Reference to a snapshotter which maintains a snapshot of the
	// entire state of the store
	snapshotter *snap.Snapshotter

	// Indicates at what log-index the current snapshot is at
	snapshotIndex uint64

	// Indicates at what index we have entries to be written to snapshot
	appliedIndex uint64

	// a threshold which triggers writing to the snapshot
	snapCount uint64

	// singals when the node is started
	nodeReadyC chan bool
}

const heartBeatInterval = 1
const electionTimeoutInterval = 10 * heartBeatInterval

func NewRaftyNode(id uint64,
	peerIds []uint64,
	raftNet *RaftNet,
	waldir string,
	snapshotter *snap.Snapshotter,
	getSnapshot func() ([]byte, error),
	commitC chan<- *string,
	proposeC <-chan string) *RaftyNode {
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

	rn := &RaftyNode{
		id:          id,
		store:       storage,
		raftNet:     raftNet,
		ctx:         context.TODO(),
		commitC:     commitC,
		proposeC:    proposeC,
		snapshotter: snapshotter,
		nodeReadyC:  make(chan bool),
		getSnapshot: getSnapshot,
		waldir:      waldir,
	}

	// ToDo: fix
	// Start a the node, a true bool value is
	// sent on readyNodeC channel
	go startNode(rn, cfg, peers)

	return rn
}

func startNode(rn *RaftyNode, cfg *raft.Config, peers []raft.Peer) {
	// assumption - the snapshotter is setup and ready
	// check to see if a wal directory exists.
	oldwalExists := wal.Exist(rn.waldir)
	// replay the WAL (for now its a snapshot)
	rn.replayWAL()
	if oldwalExists {
		rn.node = raft.RestartNode(cfg)
		log.Infof("re-started node %v", rn.node)
	} else {
		// start the node (since there is no WAL
		rn.node = raft.StartNode(cfg, peers)
		log.Infof("started node %v", rn.node)
	}

	// signal that this node is ready
	rn.nodeReadyC <- true
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

// replayWAL replays WAL entries into the raft instance.
// returns an instance of the WAL pointer
func (rn *RaftyNode) replayWAL() *wal.WAL {
	log.Infof("replaying WAL of member %d", rn.id)
	// load the snapshot from the disk
	snapshot := rn.loadSnapshot()

	// open the write ahead log, the snapshot provides which
	// index and term log to load from
	// see the doc for WAL
	// https://godoc.org/github.com/coreos/etcd/wal
	w := rn.openWAL(snapshot)

	// This will give you the metadata, the last raft.State (aka. hardState)
	// and the slice of raft.Entry items in the log.
	metadata, lastHardState, entries, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}

	log.Infof("metadata from replayWAL %v", len(metadata))
	if snapshot != nil {
		err := rn.store.ApplySnapshot(*snapshot)
		if err != nil {
			log.Fatalf("store.ApplySnapshot err=%v", err)
		}
	}

	err = rn.store.SetHardState(lastHardState)
	if err != nil {
		log.Fatalf("store.SetHardState() err=%v", err)
	}

	// append to storage so raft starts at the right place in log
	err = rn.store.Append(entries)
	if err != nil {
		log.Fatalf("store.Append(..) err=%v", err)
	}

	// send nil once lastIndex is published so client knows commit
	// channel is current
	if len(entries) > 0 {
		rn.lastIndex = entries[len(entries)-1].Index
	} else {
		// ToDo: understand why nil is sent only in the else case  s
		rn.commitC <- nil
	}

	return w
}

// openWAL returns a WAL ready for reading.
func (rn *RaftyNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rn.waldir) {
		log.Warnf("wal directory %s not found", rn.waldir)
		if err := os.Mkdir(rn.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rn.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}

		err = w.Close()
		if err != nil {
			log.Fatalf("raftexample: error in w.close() err=%v", err)
		}
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}

	log.Printf("loading WAL at term %d and index %d",
		walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rn.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

func (rn *RaftyNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rn.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	} else if err == snap.ErrNoSnapshot {
		log.Warnf("No snapshot found!!!")
	}

	return snapshot
}

func (rn *RaftyNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rn.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rn.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rn.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftyNode) maybeTriggerSnapshot() {
	// log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	// data, err := rc.getSnapshot()
	// if err != nil {
	// 	log.Panic(err)
	// }
	// snap, err := rc.store.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	// if err != nil {
	// 	panic(err)
	// }
	// if err := rc.saveSnap(snap); err != nil {
	// 	panic(err)
	// }
	//
	// compactIndex := uint64(1)
	// if rc.appliedIndex > snapshotCatchUpEntriesN {
	// 	compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	// }
	// if err := rc.raftStorage.Compact(compactIndex); err != nil {
	// 	panic(err)
	// }
	//
	// log.Printf("compacted log at index %d", compactIndex)
	// rc.snapshotIndex = rc.appliedIndex
}

func (rn *RaftyNode) saveToStorage(hardState raftpb.HardState,
	entries []raftpb.Entry, snapshot raftpb.Snapshot) {
	if len(entries) > 0 {
		log.Warnf("entries is not empty empty!!!")
	}

	err := rn.store.Append(entries)
	if err != nil {
		log.Panicf("rn.store.Append error=%v", err)
	}

	if !raft.IsEmptyHardState(hardState) {
		log.Warnf("hardstate is not empty")
		err := rn.store.SetHardState(hardState)
		if err != nil {
			log.Panicf("rn.store.SetHardState error=%v", err)
		}
	}

	if !raft.IsEmptySnap(snapshot) {
		log.Warnf("saving snapshot...")
		err := rn.saveSnap(snapshot)
		if err != nil {
			log.Panicf("rn.store.ApplySnapshot err=%v", err)
		}

		err = rn.store.ApplySnapshot(snapshot)
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
