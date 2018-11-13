package circular

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	maximumIterations = 8
)

var (
	operationsIterations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore_circular",
			Name:      "file_offset_store_operations_iterations",
			Help:      "Iterations spent per operation on the file offset store.",
			Buckets:   prometheus.LinearBuckets(1.0, 1.0, maximumIterations),
		},
		[]string{"operation", "result"})
)

func init() {
	prometheus.MustRegister(operationsIterations)
}

type offsetRecord [sha256.Size + 4 + 4 + 8]byte

func newOffsetRecord(digest *util.Digest, offset uint64) offsetRecord {
	var offsetRecord offsetRecord
	copy(offsetRecord[:sha256.Size], digest.GetHash())
	binary.LittleEndian.PutUint32(offsetRecord[sha256.Size:], uint32(digest.GetSizeBytes()))
	binary.LittleEndian.PutUint64(offsetRecord[sha256.Size+8:], offset)
	return offsetRecord
}

func (or *offsetRecord) getSlot() uint32 {
	slot := uint32(2166136261)
	for i := sha256.Size + 4 + 4; i > 0; i-- {
		slot ^= uint32(or[i-1])
		slot *= 16777619
	}
	return slot
}

func (or *offsetRecord) getAttempt() uint32 {
	return binary.LittleEndian.Uint32(or[sha256.Size+4:])
}

func (or *offsetRecord) getOffset() uint64 {
	return binary.LittleEndian.Uint64(or[sha256.Size+4+4:])
}

func (or *offsetRecord) digestAndAttemptEqual(other offsetRecord) bool {
	return bytes.Equal(or[:sha256.Size+4+4], other[:sha256.Size+4+4])
}

func (or *offsetRecord) offsetInBounds(minOffset uint64, maxOffset uint64) bool {
	offset := or.getOffset()
	return offset >= minOffset && offset <= maxOffset
}

func (or *offsetRecord) withAttempt(attempt uint32) offsetRecord {
	newRecord := *or
	binary.LittleEndian.PutUint32(newRecord[sha256.Size+4:], attempt)
	return newRecord
}

type fileOffsetStore struct {
	file ReadWriterAt
	size uint64
}

func NewFileOffsetStore(file ReadWriterAt, size uint64) OffsetStore {
	return &fileOffsetStore{
		file: file,
		size: size,
	}
}

func (os *fileOffsetStore) getPositionOfSlot(slot uint32) int64 {
	recordLen := uint64(len(offsetRecord{}))
	return int64((uint64(slot) % (os.size / recordLen)) * recordLen)
}

func (os *fileOffsetStore) getRecordAtPosition(position int64) (offsetRecord, error) {
	var record offsetRecord
	if _, err := os.file.ReadAt(record[:], position); err != nil && err != io.EOF {
		return record, err
	}
	return record, nil
}

func (os *fileOffsetStore) putRecordAtPosition(record offsetRecord, position int64) error {
	_, err := os.file.WriteAt(record[:], position)
	return err
}

func (os *fileOffsetStore) Get(digest *util.Digest, minOffset uint64, maxOffset uint64) (uint64, bool, error) {
	record := newOffsetRecord(digest, 0)
	for iteration := uint32(1); ; iteration++ {
		if iteration >= maximumIterations {
			operationsIterations.WithLabelValues("Get", "TooManyIterations").Observe(float64(iteration))
			return 0, false, nil
		}

		lookupRecord := record.withAttempt(iteration - 1)
		position := os.getPositionOfSlot(lookupRecord.getSlot())
		storedRecord, err := os.getRecordAtPosition(position)
		if err != nil {
			operationsIterations.WithLabelValues("Get", "Error").Observe(float64(iteration))
			return 0, false, err
		}
		if !storedRecord.offsetInBounds(minOffset, maxOffset) {
			operationsIterations.WithLabelValues("Get", "NotFound").Observe(float64(iteration))
			return 0, false, nil
		}
		if storedRecord.digestAndAttemptEqual(lookupRecord) {
			operationsIterations.WithLabelValues("Get", "Success").Observe(float64(iteration))
			return storedRecord.getOffset(), true, nil
		}
		if os.getPositionOfSlot(storedRecord.getSlot()) != position {
			operationsIterations.WithLabelValues("Get", "NotFound").Observe(float64(iteration))
			return 0, false, nil
		}
	}
}

func (os *fileOffsetStore) putRecord(record offsetRecord, minOffset uint64, maxOffset uint64) (offsetRecord, bool, error) {
	position := os.getPositionOfSlot(record.getSlot())

	// Fetch the old record. If it is invalid, or already at a spot where it
	// can't be moved to another place, simply overwrite it.
	oldRecord, err := os.getRecordAtPosition(position)
	if err != nil {
		return offsetRecord{}, false, err
	}
	oldAttempt := oldRecord.getAttempt()
	if !oldRecord.offsetInBounds(minOffset, maxOffset) ||
		oldAttempt >= maximumIterations-1 ||
		os.getPositionOfSlot(oldRecord.getSlot()) != position {
		return offsetRecord{}, false, os.putRecordAtPosition(record, position)
	}

	if oldRecord.getOffset() <= record.getOffset() {
		return oldRecord.withAttempt(oldAttempt + 1), true, os.putRecordAtPosition(record, position)
	}

	attempt := record.getAttempt()
	if attempt >= maximumIterations-1 {
		return offsetRecord{}, false, nil
	}
	return record.withAttempt(attempt + 1), true, nil
}

func (os *fileOffsetStore) Put(digest *util.Digest, offset uint64, minOffset uint64, maxOffset uint64) error {
	record := newOffsetRecord(digest, offset)
	for iteration := 1; ; iteration++ {
		if iteration > maximumIterations {
			operationsIterations.WithLabelValues("Put", "TooManyIterations").Observe(float64(iteration))
			return nil
		}

		if nextRecord, more, err := os.putRecord(record, minOffset, maxOffset); err != nil {
			operationsIterations.WithLabelValues("Put", "Error").Observe(float64(iteration))
			return err
		} else if more {
			record = nextRecord
		} else {
			operationsIterations.WithLabelValues("Put", "Success").Observe(float64(iteration))
			return nil
		}
	}
}
