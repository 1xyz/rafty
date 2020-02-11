package main

import (
	log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/wal"
	"go.uber.org/zap"
)

// refer: https://godoc.org/github.com/coreos/etcd/wal
func walScratch() {
	var metadata = []byte{}
	wal, err := wal.Create(zap.NewExample(), "/tmp/rafty", metadata)
	if err != nil {
		log.Panicf("error wal.create %v", err)
	}

	// wal.Save()
	//
	// log.Infof(wal.)

}

func main() {
	walScratch()
}
