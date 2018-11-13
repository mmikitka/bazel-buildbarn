package circular

import (
	"context"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/EdSchouten/bazel-buildbarn/pkg/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type OffsetStore interface {
	Get(digest *util.Digest, minOffset uint64, maxOffset uint64) (uint64, bool, error)
	Put(digest *util.Digest, offset uint64, minOffset uint64, maxOffset uint64) error
}

type DataStore interface {
	Put(r io.Reader, offset uint64) error
	Get(offset uint64, size int64) io.ReadCloser
}

type StateStore interface {
	Get() (uint64, uint64, error)
	Put(readCursor uint64, writeCursor uint64) error
}

type circularBlobAccess struct {
	// Fields that are constant or lockless.
	dataStore DataStore
	dataSize  uint64

	// Fields protected by the lock.
	lock        sync.Mutex
	readCursor  uint64
	writeCursor uint64
	offsetStore OffsetStore
	stateStore  StateStore
}

func NewCircularBlobAccess(offsetStore OffsetStore, dataStore DataStore, dataSize uint64, stateStore StateStore) (blobstore.BlobAccess, error) {
	readCursor, writeCursor, err := stateStore.Get()
	if err != nil {
		return nil, err
	}

	ba := &circularBlobAccess{
		offsetStore: offsetStore,
		dataStore:   dataStore,
		dataSize:    dataSize,
		stateStore:  stateStore,

		readCursor:  readCursor,
		writeCursor: writeCursor,
	}
	go ba.flushStateStore()
	return ba, nil
}

func (ba *circularBlobAccess) flushStateStore() {
	for {
		time.Sleep(time.Minute)

		ba.lock.Lock()
		readCursor := ba.readCursor
		writeCursor := ba.writeCursor
		ba.lock.Unlock()

		if err := ba.stateStore.Put(readCursor, writeCursor); err != nil {
			log.Print("Failed to write to state store: ", err)
		}
	}
}

func (ba *circularBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	ba.lock.Lock()
	offset, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor)
	ba.lock.Unlock()
	if err != nil {
		return util.NewErrorReader(err)
	} else if ok {
		return ba.dataStore.Get(offset, digest.GetSizeBytes())
	}
	return util.NewErrorReader(status.Errorf(codes.NotFound, "Blob not found"))
}

func (ba *circularBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	defer r.Close()

	// Discard writes for blobs that already exist.
	ba.lock.Lock()
	if _, ok, err := ba.offsetStore.Get(digest, ba.readCursor, ba.writeCursor); err != nil {
		ba.lock.Unlock()
		return err
	} else if ok {
		ba.lock.Unlock()
		_, err := io.Copy(ioutil.Discard, r)
		return err
	}

	// Allocate space in the data store.
	offset := ba.writeCursor
	ba.writeCursor += uint64(sizeBytes)
	if ba.readCursor > ba.writeCursor {
		ba.readCursor = ba.writeCursor
	} else if ba.readCursor+ba.dataSize < ba.writeCursor {
		ba.readCursor = ba.writeCursor - ba.dataSize
	}
	ba.lock.Unlock()

	// Write the data to storage and make it visible.
	if err := ba.dataStore.Put(r, offset); err != nil {
		return err
	}
	ba.lock.Lock()
	err := ba.offsetStore.Put(digest, offset, ba.readCursor, ba.writeCursor)
	ba.lock.Unlock()
	return err

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
	}
	return nil
}

func (ba *circularBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	ba.lock.Lock()
	defer ba.lock.Unlock()

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
