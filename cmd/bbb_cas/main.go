package main

import (
	"context"
	"log"
	"net"
	"os"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore/circular"
)

func main() {
	offsetFile, err := os.OpenFile("/tmp/bbb_cas.offset", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

	dataFile, err := os.OpenFile("/tmp/bbb_cas.data", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}
	dataSize := uint64(1024 * 1024 * 1024)

	stateFile, err := os.OpenFile("/tmp/bbb_cas.state", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
	}

	blobAccess, err := circular.NewCircularBlobAccess(
		circular.NewFileOffsetStore(offsetFile, 1024*1024),
		circular.NewFileDataStore(dataFile, dataSize),
		dataSize,
		circular.NewFileStateStore(stateFile))
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
