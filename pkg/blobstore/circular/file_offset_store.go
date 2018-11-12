package circular

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
)

const (
	slotsPerKey = 10
	maximumIterationsPerPut = 100
)

type offsetKey [sha256.Size + 4 + 4 + 8]byte

func newOffsetKey(digest *util.Digest, offset uint64) offsetKey {
	var offsetKey offsetKey
	copy(offsetKey[:sha256.Size], digest.GetHash())
	binary.LittleEndian.PutUint32(offsetKey[sha256.Size:], uint32(digest.GetSizeBytes()))
	binary.LittleEndian.PutUint64(offsetKey[sha256.Size+8:], offset)
	return offsetKey
}

type slotsList [sha256.Size / 4]uint32

func (ok *offsetKey) getSlot() uint32 {
	slot := uint32(2166136261)
	for _, b := range ok[:sha256.Size + 8] {
		slot ^= uint32(b)
		slot *= 16777619
	}
	return slot
}

func (ok *offsetKey) getAttempt() uint32 {
	return binary.LittleEndian.Uint32(ok[sha256.Size+4:])
}

func (ok *offsetKey) getOffset() uint64 {
	return binary.LittleEndian.Uint64(ok[sha256.Size+8:])
}

func (ok *offsetKey) digestAndAttemptEqual(other offsetKey) bool {
	return bytes.Equal(ok[:sha256.Size+8], other[:sha256.Size+8])
}

func (ok *offsetKey) offsetInBounds(minOffset uint64, maxOffset uint64) bool {
	offset := ok.getOffset()
	return offset >= minOffset || offset <= maxOffset
}

func (ok *offsetKey) withAttempt(attempt uint32) offsetKey {
	newKey := *ok
	binary.LittleEndian.PutUint32(newKey[sha256.Size+4:], attempt)
	return newKey
}

type fileOffsetStore struct {
	file *os.File
	size uint64
}

func NewFileOffsetStore(path string, size uint64) (OffsetStore, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &fileOffsetStore{
		file: file,
		size: size,
	}, nil
}

func (os *fileOffsetStore) getPositionOfSlot(slot uint32) int64 {
	keyLen := uint64(len(offsetKey{}))
	return int64((uint64(slot) % (os.size / keyLen)) * keyLen)
}

func (os *fileOffsetStore) getKeyAtPosition(position int64) (offsetKey, error) {
	var key offsetKey
	if _, err := os.file.ReadAt(key[:], position); err != nil && err != io.EOF {
		return key, err
	}
	return key, nil
}

func (os *fileOffsetStore) putKeyAtPosition(key offsetKey, position int64) error {
	_, err := os.file.WriteAt(key[:], position)
	return err
}

func (os *fileOffsetStore) Get(digest *util.Digest, minOffset uint64, maxOffset uint64) (uint64, bool, error) {
	key := newOffsetKey(digest, 0)
	for attempt := uint32(0); attempt < slotsPerKey; attempt++ {
		lookupKey := key.withAttempt(attempt)
		slot := lookupKey.getSlot()
		position := os.getPositionOfSlot(slot)
		storedKey, err := os.getKeyAtPosition(position)
		if err != nil {
			return 0, false, err
		}
		if !storedKey.offsetInBounds(minOffset, maxOffset) {
			break
		}
		if storedKey.digestAndAttemptEqual(lookupKey) {
			return storedKey.getOffset(), true, nil
		}
	}
	return 0, false, nil
}

func (os *fileOffsetStore) putKey(key offsetKey, minOffset uint64, maxOffset uint64) (offsetKey, bool, error) {
	slot := key.getSlot()
	position := os.getPositionOfSlot(slot)

	// Fetch the old key. If it is invalid, or already at a spot where it
	// can't be moved to another place, simply overwrite it.
	oldKey, err := os.getKeyAtPosition(position)
	if err != nil {
		return offsetKey{}, false, err
	}
	oldAttempt := oldKey.getAttempt()
	if !oldKey.offsetInBounds(minOffset, maxOffset) ||
		oldAttempt >= slotsPerKey - 1 ||
		os.getPositionOfSlot(oldKey.getSlot()) != position {
		return offsetKey{}, false, os.putKeyAtPosition(key, position)
	}

	if oldKey.getOffset() <= key.getOffset() {
		return oldKey.withAttempt(oldAttempt + 1), true, os.putKeyAtPosition(key, position)
	}

	attempt := key.getAttempt()
	if attempt >= slotsPerKey - 1 {
		return offsetKey{}, false, nil
	}
	return key.withAttempt(attempt + 1), true, nil
}

func (os *fileOffsetStore) Put(digest *util.Digest, minOffset uint64, newOffset uint64) error {
	key := newOffsetKey(digest, newOffset)
	for i := 0; i < maximumIterationsPerPut; i++ {
		if nextKey, more, err := os.putKey(key, minOffset, newOffset); err != nil {
			return err
		} else if more {
			key = nextKey
		} else {
			break
		}
	}
	return nil
}
