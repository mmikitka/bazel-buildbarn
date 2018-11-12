package circular

import (
	"io"
)

type fileDataStore struct {
	file ReadWriterAt
	size uint64
}

func NewFileDataStore(file ReadWriterAt, size uint64) DataStore {
	return &fileDataStore{
		file: file,
		size: size,
	}
}

func (ds *fileDataStore) Put(b []byte, offset uint64) error {
	for len(b) > 0 {
		writeOffset := offset % ds.size
		writeLength := uint64(len(b))
		if writeLength > ds.size-writeOffset {
			writeLength = ds.size - writeOffset
		}
		if _, err := ds.file.WriteAt(b[:writeLength], int64(writeOffset)); err != nil {
			return err
		}
		b = b[writeLength:]
		offset += writeLength
	}
	return nil
}

func (ds *fileDataStore) Get(offset uint64, size int64) io.ReadCloser {
	return &fileDataStoreReader{
		ds:     ds,
		offset: offset,
		size:   uint64(size),
	}
}

type fileDataStoreReader struct {
	ds     *fileDataStore
	offset uint64
	size   uint64
}

func (f *fileDataStoreReader) Read(b []byte) (n int, err error) {
	if f.size == 0 {
		return 0, io.EOF
	}

	readOffset := f.offset % f.ds.size
	readLength := f.size
	if bLength := uint64(len(b)); readLength > bLength {
		readLength = bLength
	}
	if readLength > f.ds.size-readOffset {
		readLength = f.ds.size - readOffset
	}

	if nRead, err := f.ds.file.ReadAt(b[:readLength], int64(readOffset)); err != nil {
		return 0, err
	}

	f.offset += readLength
	f.size -= readLength
	return int(readLength), nil
}

func (f *fileDataStoreReader) Close() error {
	return nil
}
