package main

import (
	"context"
	"flag"
	"log"
	"net"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore/configuration"
)

func main() {
	var (
		blobstoreConfig = flag.String("blobstore-config", "/config/blobstore.conf", "Configuration for blob storage")
	)
	flag.Parse()

	// Storage access.
	blobAccess, _, err := configuration.CreateBlobAccessObjectsFromConfig(*blobstoreConfig, false)
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}
	rs := blobstore.NewRedisServer(blobAccess)

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
