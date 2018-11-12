package main

import (
	"context"
	"log"
	"net"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore/circular"
)

func main() {
	dataSize := uint64(1024 * 1024 * 1024)
	dataStore, err := circular.NewFileDataStore("/tmp/bbb_cas.data", dataSize)
	if err != nil {
		log.Fatal(err)
	}
	stateStore, err := circular.NewFileStateStore("/tmp/bbb_cas.state")
	if err != nil {
		log.Fatal(err)
	}

	blobAccess, err := circular.NewCircularBlobAccess(nil, dataStore, dataSize, stateStore)
	if err != nil {
		log.Fatal(err)
	}

	rs := blobstore.NewRedisServer(blobstore.NewMerkleBlobAccess(blobAccess))

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := ln.Accept()
		if err == nil {
			go rs.HandleConnection(context.Background(), conn)
		} else {
			log.Print(err)
		}
	}
}
