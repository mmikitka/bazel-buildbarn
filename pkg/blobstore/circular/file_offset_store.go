package circular

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"os"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
)

type offsetKey [sha256.Size + 4 + 4 + 8]byte

func newOffsetKey(digest *util.Digest, attempt int, offset uint64) offsetKey {
	var offsetKey offsetKey
	copy(offsetKey[:sha256.Size], digest.GetHash())
	binary.LittleEndian.PutUint32(offsetKey[sha256.Size:], uint32(digest.GetSizeBytes()))
	binary.LittleEndian.PutUint32(offsetKey[sha256.Size+4:], uint32(attempt))
	binary.LittleEndian.PutUint64(offsetKey[sha256.Size+8:], offset)
	return offsetKey
}

type slotsList [sha256.Size / 4]uint32

func (ok *offsetKey) getSlotsList() slotsList {
	var slotsList slotsList
	for i := 0; i < len(slotsList); i++ {
		slotsList[i] = binary.LittleEndian.Uint32(ok[i*4:])
	}
	return slotsList
}

func (ok *offsetKey) getOffset() uint64 {
	return binary.LittleEndian.Uint64(ok[sha256.Size+8:])
}

func (ok *offsetKey) digestEqual(other offsetKey) bool {
	return bytes.Equal(ok[:sha256.Size+8], other[:sha256.Size+8])
}

func (ok *offsetKey) offsetInBounds(minOffset uint64, maxOffset uint64) bool {
	offset := ok.getOffset()
	return offset >= minOffset || offset <= maxOffset
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
	lookupKey := newOffsetKey(digest, 0, 0)
	for attempt, slot := range lookupKey.getSlotsList() {
		position := os.getPositionOfSlot(slot)
		storedKey, err := os.getKeyAtPosition(position)
		if err != nil {
			return 0, false, err
		}
		if !storedKey.offsetInBounds(minOffset, maxOffset) {
			break
		}
		if storedKey.digestEqual(lookupKey) && storedKey.attemptEqual(attempt) {
			return storedKey.getOffset(), true, nil
		}
	}
	return 0, false, nil
}

func (os *fileOffsetStore) pushKeyToNextSlot(key offsetKey, minOffset uint64, maxOffset uint64) error {
	if offset := key.getOffset(); offset < minOffset || offset > maxOffset {
		return nil
	}
	slotsList := key.getSlotsList()
	currentAttempt := key.getAttempt()
	if currentAttempt < 0 || currentAttempt >= len(slotsList) ||
		os.getPositionOfSlot(slotsList[currentAttempt]) != position {
		return nil
	}

	for attempt := oldAttempt + 1; attempt < len(slotsList); attempt++ {
	}
}

func (os *fileOffsetStore) Put(digest *util.Digest, minOffset uint64, newOffset uint64) error {
	key := newOffsetKey(digest, newOffset)
	slotsList := key.getSlotsList()
	position := os.getPositionOfSlot(slotsList[0])
	oldKey, err := os.getKeyAtPosition(position)
	if err != nil {
		return err
	}
	if err := os.putKeyAtPosition(key, position); err != nil {
		return err
	}
	return os.pushKeyToNextSlot(oldKey, minOffset, newOffset)
}
