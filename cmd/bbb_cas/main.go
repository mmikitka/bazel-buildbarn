package main

import (
	"log"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore/circular"
)

func main() {
	_, err := circular.NewCircularBlobAccess(nil, nil, 0, nil)
	if err != nil {
		log.Fatal(err)
	}
}
