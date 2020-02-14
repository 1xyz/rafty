package main

import (
	"github.com/1xyz/rafty/core"
	"github.com/docopt/docopt-go"
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

func toInt(opts docopt.Opts, key string) uint64 {
	r, err := opts.Int(key)
	if err != nil {
		log.Panicf("opts.Int(id) %v", err)
	}
	return uint64(r)
}

func init() {
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})
	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func propose(raftNet *core.RaftNet, nodeId uint64, key, value string) {
	log.Errorf("Propose changes.....")
	raftNet.ProposeChange(nodeId, key, value)
	log.Infof("** Sleeping to visualize heartbeat between nodes **")
	time.Sleep(2000 * time.Millisecond)
	for k, v := range raftNet.ReadFromAllNodes(key) {
		log.Errorf("** Result nodeID=%v key=%v value=%v", k, key, v)
	}
}

func proposeWithMultipleChanges(raftNet *core.RaftNet) {
	log.Errorf("Propose changes.....")

	key := "beans"
	raftNet.ProposeChange(1, key, "garbanzo")
	raftNet.ProposeChange(2, key, "lima")
	// raftNet.ProposeChange(2, key, "lima")
	time.Sleep(10 * time.Millisecond)
	for k, v := range raftNet.ReadFromAllNodes(key) {
		log.Errorf("** Result nodeID=%v key=%v value=%v", k, key, v)
	}
	raftNet.ProposeChange(2, key, "rajma")

	log.Infof("** Sleeping to visualize heartbeat between nodes **")
	time.Sleep(2000 * time.Millisecond)
	for k, v := range raftNet.ReadFromAllNodes(key) {
		log.Errorf("** Result nodeID=%v key=%v value=%v", k, key, v)
	}
}

func main() {
	usage := `Rafty.

Usage:
  rafty standalone <node-count>
  rafty -h | --help

Options:
  -h --help     Show this screen.`
	argv := os.Args[1:]
	parser := &docopt.Parser{
		HelpHandler:  docopt.PrintHelpOnly,
		OptionsFirst: true,
	}

	opts, err := parser.ParseArgs(usage, argv, "")
	if err != nil {
		log.Errorf("error %v", err)
		return
	}

	standalone, err := opts.Bool("standalone")
	if err != nil {
		log.Errorf("opts.Bool(standalone) err=%v", err)
		return
	}

	if standalone {
		nc := toInt(opts, "<node-count>")
		log.Infof("node-count = %d", nc)
		raftNet := core.NewRaftNet(nc)
		raftNet.Run()
		propose(raftNet, 1, "hello", "world")
		propose(raftNet, 2, "hello", "world2")
		proposeWithMultipleChanges(raftNet)
		// time.Sleep(60 * time.Second)
	}
}
