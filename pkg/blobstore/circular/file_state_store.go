package circular

import (
	"encoding/binary"
	"io"
	"log"
)

type fileStateStore struct {
	file ReadWriterAt
}

func NewFileStateStore(file ReadWriterAt) StateStore {
	return &fileStateStore{
		file: file,
	}
}

func (ss *fileStateStore) Get() (uint64, uint64, error) {
	var data [16]byte
	if _, err := ss.file.ReadAt(data[:], 0); err == io.EOF {
		return 0, 0, nil
	} else if err != nil {
		return 0, 0, err
	}
	readCursor := binary.LittleEndian.Uint64(data[:])
	writeCursor := binary.LittleEndian.Uint64(data[8:])
	if readCursor > writeCursor {
		return 0, 0, nil
	}
	return readCursor, writeCursor, nil
}

func (ss *fileStateStore) Put(readCursor uint64, writeCursor uint64) error {
	if readCursor > writeCursor {
		log.Fatalf("Attempted to write cursors %d > %d", readCursor, writeCursor)
	}
	var data [16]byte
	binary.LittleEndian.PutUint64(data[:], readCursor)
	binary.LittleEndian.PutUint64(data[8:], writeCursor)
	_, err := ss.file.WriteAt(data[:], 0)
	return err
}
