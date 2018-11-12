package circular

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"sync"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OffsetStore interface {
	Get(digest *util.Digest, minOffset uint64, maxOffset uint64) (uint64, bool, error)
	Put(digest *util.Digest, minOffset uint64, newOffset uint64) error
}

type DataStore interface {
	Put(b []byte, offset uint64) error
	Get(offset uint64, size int64) io.ReadCloser
}

type StateStore interface {
	Get() (uint64, uint64, error)
	Put(readCursor uint64, writeCursor uint64) error
}

type circularBlobAccess struct {
	offsetStore OffsetStore
	dataStore   DataStore
	dataSize    uint64
	stateStore  StateStore

	lock        sync.RWMutex
	readCursor  uint64
	writeCursor uint64
}

func NewCircularBlobAccess(offsetStore OffsetStore, dataStore DataStore, dataSize uint64, stateStore StateStore) (blobstore.BlobAccess, error) {
	readCursor, writeCursor, err := stateStore.Get()
	if err != nil {
		return nil, err
	}

	return &circularBlobAccess{
		offsetStore: offsetStore,
		dataStore:   dataStore,
		dataSize:    dataSize,
		stateStore:  stateStore,

		readCursor:  readCursor,
		writeCursor: writeCursor,
	}, nil
}

func (ba *circularBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	ba.lock.RLock()
	defer ba.lock.RUnlock()

	if offset, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor); err != nil {
		return util.NewErrorReader(err)
	} else if ok {
		return ba.dataStore.Get(offset, digest.GetSizeBytes())
	}
	return util.NewErrorReader(status.Errorf(codes.NotFound, "Blob not found"))
}

func (ba *circularBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	// Read all data up front to ensure we don't need to hold locks for a long time.
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	if l := int64(len(data)); l != digest.GetSizeBytes() || l != sizeBytes {
		log.Fatal("Called into CAS to store non-CAS object")
	}

	ba.lock.Lock()
	defer ba.lock.Unlock()

	// Ignore the write in case the blob is already present in storage.
	if _, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor); err != nil {
		return err
	} else if ok {
		return nil
	}

	if err := ba.dataStore.Put(data, ba.writeCursor); err != nil {
		return err
	}
	if err := ba.offsetStore.Put(digest, ba.readCursor, ba.writeCursor); err != nil {
		return err
	}

	ba.writeCursor += uint64(len(data))
	if ba.readCursor > ba.writeCursor {
		ba.readCursor = ba.writeCursor
	} else if ba.readCursor+ba.dataSize < ba.writeCursor {
		ba.readCursor = ba.writeCursor - ba.dataSize
	}
	return ba.stateStore.Put(ba.readCursor, ba.writeCursor)
}

func (ba *circularBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	if offset, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor); err != nil {
		return err
	} else if ok {
		ba.readCursor = offset + 1
		if ba.writeCursor < ba.readCursor {
			ba.writeCursor = ba.readCursor
		}
		return ba.stateStore.Put(ba.readCursor, ba.writeCursor)
	}
	return nil
}

func (ba *circularBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ba.lock.RLock()
	defer ba.lock.RUnlock()

	var missingDigests []*util.Digest
	for _, digest := range digests {
		if _, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor); err != nil {
			return nil, err
		} else if !ok {
			missingDigests = append(missingDigests, digest)
		}
	}
	return missingDigests, nil
}
