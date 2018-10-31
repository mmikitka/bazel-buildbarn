package blobstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"hash"
	"io"
	"log"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type merkleBlobAccess struct {
	BlobAccess
}

// NewMerkleBlobAccess creates an adapter that validates that blobs read
// from and written to storage correspond with the digest that is used
// for identification. It ensures that the size and the SHA-256 based
// checksum match. This is used to ensure clients cannot corrupt the CAS
// and that if corruption were to occur, use of corrupted data is prevented.
func NewMerkleBlobAccess(blobAccess BlobAccess) BlobAccess {
	return &merkleBlobAccess{
		BlobAccess: blobAccess,
	}
}

func (ba *merkleBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	return newChecksumValidatingReader(
		digest,
		ba.BlobAccess.Get(ctx, digest),
		func() {
			// Trigger blob deletion in case we detect data
			// corruption. This will cause future calls to
			// FindMissing() to indicate absence, causing clients to
			// re-upload them and/or build actions to be retried.
			if err := ba.BlobAccess.Delete(ctx, digest); err == nil {
				log.Printf("Successfully deleted corrupted blob %s", digest)
			} else {
				log.Printf("Failed to delete corrupted blob %s: %s", digest, err)
			}
		},
		codes.Internal)
}

func (ba *merkleBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	digestSizeBytes := digest.GetSizeBytes()
	if digestSizeBytes != sizeBytes {
		log.Fatal("Called into CAS to store non-CAS object")
	}
	return ba.BlobAccess.Put(
		ctx, digest, digestSizeBytes,
		newChecksumValidatingReader(digest, r, func() {}, codes.InvalidArgument))
}

type checksumValidatingReader struct {
	io.ReadCloser

	expectedChecksum []byte
	partialChecksum  hash.Hash
	sizeLeft         int64

	// Called whenever size/checksum inconsistencies are detected.
	invalidator func()
	errorCode   codes.Code
}

func newChecksumValidatingReader(digest *util.Digest, r io.ReadCloser, invalidator func(), errorCode codes.Code) io.ReadCloser {
	return &checksumValidatingReader{
		ReadCloser:       r,
		expectedChecksum: digest.GetHash(),
		partialChecksum:  digest.NewHasher(),
		sizeLeft:         digest.GetSizeBytes(),
		invalidator:      invalidator,
		errorCode:        errorCode,
	}
}

func (r *checksumValidatingReader) Read(p []byte) (int, error) {
	n, err := io.TeeReader(r.ReadCloser, r.partialChecksum).Read(p)
	nLen := int64(n)
	if nLen > r.sizeLeft {
		r.invalidator()
		return 0, status.Error(r.errorCode, "Blob is longer than expected")
	}
	r.sizeLeft -= nLen

	if err == io.EOF {
		if r.sizeLeft != 0 {
			r.invalidator()
			return 0, status.Errorf(r.errorCode, "Blob is %d bytes shorter than expected", r.sizeLeft)
		}

		actualChecksum := r.partialChecksum.Sum(nil)
		if bytes.Compare(actualChecksum, r.expectedChecksum) != 0 {
			r.invalidator()
			return 0, status.Errorf(
				r.errorCode,
				"Checksum of blob is %s, while %s was expected",
				hex.EncodeToString(actualChecksum),
				hex.EncodeToString(r.expectedChecksum))
		}
	}
	return n, err
}
