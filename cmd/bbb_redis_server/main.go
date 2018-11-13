package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore/configuration"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	var (
		blobstoreConfig = flag.String("blobstore-config", "/config/blobstore.conf", "Configuration for blob storage")
	)
	flag.Parse()

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":80", nil))
	}()

	// Storage access.
	blobAccess, _, err := configuration.CreateBlobAccessObjectsFromConfig(*blobstoreConfig, false)
	if err != nil {
		log.Fatal("Failed to create blob access: ", err)
	}
	rs := blobstore.NewRedisServer(blobAccess)

	sock, err := net.Listen("tcp", ":6379")
	if err != nil {
		log.Fatal("Failed to create listening socket: ", err)
	}
	for {
		conn, err := sock.Accept()
		if err == nil {
			go rs.HandleConnection(context.Background(), conn)
		} else {
			log.Print("Failed to accept incoming connection: ", err)
		}
	}
}
