package blobstore

import (
	"context"
	"io"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
)

// BlobAccess is an abstraction for a data store that can be used to
// hold both a Bazel Action Cache (AC) and Content Addressable Storage
// (CAS).
type BlobAccess interface {
	Get(ctx context.Context, digest *util.Digest) io.ReadCloser
	Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error
	Delete(ctx context.Context, digest *util.Digest) error
	FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error)
}
