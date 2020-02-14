package core

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"sync"
)

type KVStore struct {
	// channel for proposing updates
	proposeC chan<- string

	// RW mutex controlling access to the store
	mu sync.RWMutex

	// reference to a snapshotter to load the data into storage
	snapShotter *snap.Snapshotter

	// underlying key value map
	store map[string]string
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *KVStore {
	s := &KVStore{
		proposeC:    proposeC,
		mu:          sync.RWMutex{},
		snapShotter: snapshotter,
		store:       make(map[string]string),
	}

	log.Infof("readCommits...")
	s.readCommits(commitC, errorC)
	// replay log into key-value map
	// s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *KVStore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.store[key]
	return v, ok
}

func (s *KVStore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Panicf("encode err=%v", err)
	}

	s.proposeC <- buf.String()
}

type kv struct {
	Key string
	Val string
}

func (s *KVStore) readCommits(commitC <-chan *string, errorC <-chan error) {
	// consume commit entries
	for data := range commitC {
		if data == nil {
			// ToDo: Fix, this is fairly vague way to load snapshots
			// ToDo: Why? Since implicit signaling via a nil.
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapShotter.Load()
			if err != nil {
				if err == snap.ErrNoSnapshot {
					return
				}

				log.Panicf("snapShotter.Load() err=%v", err)
			}

			log.Infof("loading snapshot at term=%d and index=%d",
				snapshot.Metadata.Term, snapshot.Metadata.Index)
			err = s.recoverFromSnapshot(snapshot.Data)
			if err != nil {
				log.Panicf("s.recoverFromSnapshot err=%v", err)
			}

			continue
		}

		var dataKv kv
		// ToDo: I don't particularly see a reason to pass a string ptr
		// ToDo: across the channel across, why not just pass bytes.
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}

		s.mu.Lock()
		s.store[dataKv.Key] = dataKv.Val
		s.mu.Unlock()
	}

	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

// Marshal the entire snapshot as a byte array. Typically this is used
// to serialize this data to disk.
func (s *KVStore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.store)
}

// Un-Marshall the entire snapshot (and load that into this store
func (s *KVStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
	return nil
}
