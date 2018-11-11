package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rawDigest is the on-disk representation of util.Digest. It is encoded
// by simply concatenating the bytes of the hash with a 32-bit size.
// Hashes smaller than SHA-256 are padded with zero bytes. As this
// storage backend should only be used by the CAS, the instance name is
// ignored.
type rawDigest [sha256.Size + 4]byte

// newRawDigest converts a util.Digest to a rawDigest.
func newRawDigest(digest *util.Digest) rawDigest {
	var r rawDigest
	copy(r[:sha256.Size], digest.GetHash())
	binary.LittleEndian.PutUint32(r[sha256.Size:], uint32(digest.GetSizeBytes()))
	return r
}

type offsetEntry [len(rawDigest{}) + 8]byte

type partialTable struct {
	offsets map[rawDigest]int64

	data     readWriterAt
	dataSize int64
}

func (t *partialTable) getBlobOffset(digest rawDigest) (int64, bool) {
	offset, ok := t.offsets[digest]
	return offset, ok
}

func (t *partialTable) getBlobReader(offset int64, length int64) io.ReadCloser {
	return ioutil.NopCloser(io.NewSectionReader(t.data, offset, length))
}

func (t *partialTable) putBlob(digest rawDigest, data []byte) error {
	n, err := t.data.WriteAt(data, t.dataSize)
	if err == nil {
		return err
	}
	t.offsets[digest] = t.dataSize
	t.dataSize += int64(n)
	return nil
}

type completeTable struct {
	bloomFilter       io.ReaderAt
	bloomFilterSize   int64
	bloomFilterRounds int

	offsets                  io.ReaderAt
	offsetsSize              int64
	offsetsMaximumIterations int

	data io.ReaderAt
}

func (t *completeTable) matchesBloomFilter(digest rawDigest) (bool, error) {
	for i := 0; i < t.bloomFilterRounds; i++ {
		hash := binary.LittleEndian.Uint32(digest[i*4 : (i+1)*4])
		offsetByte := int64(hash/8) % t.bloomFilterSize
		offsetBit := hash % 8
		var b [1]byte
		if _, err := t.bloomFilter.ReadAt(b[:], offsetByte); err == io.EOF {
			// Malformed bloom filter.
			return false, nil
		} else if err != nil {
			// I/O error.
			return false, err
		} else if b[0]&(1<<offsetBit) == 0 {
			// Digest not part of bloom filter.
			return false, nil
		}
	}
	return true, nil
}

func (t *completeTable) getBlobOffset(digest rawDigest) (int64, bool, error) {
	slots := t.offsetsSize / int64(len(offsetEntry{}))
	hash := binary.LittleEndian.Uint32(digest[0:4])
	for i := 0; i < t.offsetsMaximumIterations; i++ {
		var offsetEntry offsetEntry
		if _, err := t.offsets.ReadAt(offsetEntry[:], (int64(hash)%slots)*int64(len(offsetEntry))); err != nil {
			return 0, false, err
		}
		if bytes.Equal(digest[:], offsetEntry[:len(digest)]) {
			return int64(binary.LittleEndian.Uint64(offsetEntry[len(digest):])), true, nil
		}
		hash += uint32(i) + 1
	}
	return 0, false, nil
}

func (t *completeTable) getBlobReader(offset int64, length int64) io.ReadCloser {
	return ioutil.NopCloser(io.NewSectionReader(t.data, offset, length))
}

type readWriterAt interface {
	io.ReaderAt
	io.WriterAt
}

type filesystemBackedBlobAccess struct {
	lock sync.RWMutex

	partialTable   partialTable
	completeTables []completeTable
}

type FilesystemAccess interface {
	ReadDir() ([]string, error)
	OpenReadMapped(filename string) (io.ReaderAt, error)
}

func NewFilesystemBackedBlobAccess(filesystemAccess FilesystemAccess) (BlobAccess, error) {
	entries, err := filesystemAccess.ReadDir()
	if err != nil {
		return nil, err
	}

	// Scan the directory to see which data is already there.
	var firstFreeIndex uint
	for _, entry := range entries {
		if !strings.HasSuffix(entry, ".index") {
			continue
		}

		// Compute first free index number for next data file.
		if index, err := strconv.ParseUint(strings.TrimSuffix(entry, ".index"), 10, 0); err != nil {
			log.Printf("Failed extract index number from file %s", entry)
			continue
		} else if firstFreeIndex <= uint(index) {
			firstFreeIndex = uint(index) + 1
		}

		_, err := filesystemAccess.OpenReadMapped(entry)
		if err != nil {
			log.Printf("Failed to open index file %s: %s", entry, err)
			continue
		}
	}

	return &filesystemBackedBlobAccess{}, nil
}

func (ba *filesystemBackedBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	rawDigest := newRawDigest(digest)

	ba.lock.RLock()
	defer ba.lock.RUnlock()

	if offset, ok := ba.partialTable.getBlobOffset(rawDigest); ok {
		return ba.partialTable.getBlobReader(offset, digest.GetSizeBytes())
	}
	for _, table := range ba.completeTables {
		if match, err := table.matchesBloomFilter(rawDigest); err != nil {
			return util.NewErrorReader(err)
		} else if match {
			if offset, ok, err := table.getBlobOffset(rawDigest); err != nil {
				return util.NewErrorReader(err)
			} else if ok {
				return table.getBlobReader(offset, digest.GetSizeBytes())
			}
		}
	}
	return util.NewErrorReader(status.Error(codes.NotFound, "Blob not found"))
}

func (ba *filesystemBackedBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	// Read all data up front to ensure we don't need to hold locks for a long time.
	data, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	if l := int64(len(data)); l != digest.GetSizeBytes() || l != sizeBytes {
		log.Fatal("Called into CAS to store non-CAS object")
	}

	rawDigest := newRawDigest(digest)

	ba.lock.Lock()
	defer ba.lock.Unlock()

	// Ignore the write if the blob is already part of the dataset.
	if _, ok := ba.partialTable.getBlobOffset(rawDigest); ok {
		return nil
	}
	for _, table := range ba.completeTables {
		if match, err := table.matchesBloomFilter(rawDigest); err != nil {
			return err
		} else if match {
			if _, ok, err := table.getBlobOffset(rawDigest); err != nil {
				return err
			} else if ok {
				return nil
			}
		}
	}

	// TODO(edsch): Flush partial table prior to trying to writing if too large!

	return ba.partialTable.putBlob(rawDigest, data)

}

func (ba *filesystemBackedBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	ba.lock.Lock()
	defer ba.lock.Unlock()

	// TODO(edsch): Implement.
	return nil
}

func (ba *filesystemBackedBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	// Convert digests to on-disk key format.
	rawDigests := make([]rawDigest, len(digests))
	for i, digest := range digests {
		rawDigests[i] = newRawDigest(digest)
	}

	ba.lock.RLock()
	defer ba.lock.RUnlock()

	// Check presence of objects in the table that is partially constructed.
	var pendingKeys []int
	for i, rawDigest := range rawDigests {
		if _, ok := ba.partialTable.getBlobOffset(rawDigest); !ok {
			pendingKeys = append(pendingKeys, i)
		}
	}

	// For remaining keys, check the on-disk tables. Loop over the
	// tables, as opposed to looping over the keys. This improves
	// locality.
	for _, table := range ba.completeTables {
		if len(pendingKeys) == 0 {
			break
		}

		// Test all remaining keys against the bloom filter.
		var matches []int
		for i, pendingKey := range pendingKeys {
			if match, err := table.matchesBloomFilter(rawDigests[pendingKey]); err != nil {
				return nil, err
			} else if match {
				matches = append(matches, i)
			}
		}

		// Look up all keys that matched the bloom filter.
		for _, match := range matches {
			if _, ok, err := table.getBlobOffset(rawDigests[pendingKeys[match]]); err != nil {
				return nil, err
			} else if ok {
				pendingKeys[match] = pendingKeys[len(pendingKeys)-1]
				pendingKeys = pendingKeys[:len(pendingKeys)-1]
			}
		}
	}

	// All keys that are still pending after inspecting all tables
	// are not part of the dataset.
	var missingDigests []*util.Digest
	for _, pendingKey := range pendingKeys {
		missingDigests = append(missingDigests, digests[pendingKey])
	}
	return missingDigests, nil
}
