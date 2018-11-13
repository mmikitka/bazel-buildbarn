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

func (ds *fileDataStore) Put(r io.Reader, offset uint64) error {
	for {
		// Read data.
		writeOffset := offset % ds.size
		var b [65536]byte
		copyLength := uint64(len(b))
		if copyLength > ds.size-writeOffset {
			copyLength = ds.size - writeOffset
		}
		n, err := r.Read(b[:])
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		// Write it to storage.
		if _, err := ds.file.WriteAt(b[:n], int64(writeOffset)); err != nil {
			return err
		}
		offset += uint64(n)
	}
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

	if _, err := f.ds.file.ReadAt(b[:readLength], int64(readOffset)); err != nil {
		return 0, err
	}

	f.offset += readLength
	f.size -= readLength
	return int(readLength), nil
}

func (f *fileDataStoreReader) Close() error {
	return nil
}
