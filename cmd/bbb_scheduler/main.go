package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/EdSchouten/bazel-buildbarn/pkg/builder"
	"github.com/EdSchouten/bazel-buildbarn/pkg/proto/scheduler"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"google.golang.org/grpc"
)

func main() {
	var (
		jobsPendingMax = flag.Uint("jobs-pending-max", 100, "Maximum number of build actions to be enqueued")
	)
	flag.Parse()

	// Web server for metrics and profiling.
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatal(http.ListenAndServe(":80", nil))
	}()

	executionServer, schedulerServer := builder.NewWorkerBuildQueue(util.DigestKeyWithInstance, *jobsPendingMax)

	// RPC server.
	s := grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
	)
	remoteexecution.RegisterExecutionServer(s, executionServer)
	scheduler.RegisterSchedulerServer(s, schedulerServer)
	grpc_prometheus.EnableHandlingTimeHistogram()
	grpc_prometheus.Register(s)

	sock, err := net.Listen("tcp", ":8981")
	if err != nil {
		log.Fatal("Failed to create listening socket: ", err)
	}
	if err := s.Serve(sock); err != nil {
		log.Fatal("Failed to serve RPC server: ", err)
	}
}
