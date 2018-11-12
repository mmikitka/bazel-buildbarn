package circular

import (
	"io"
	"os"
)

type fileDataStore struct {
	file *os.File
	size uint64
}

func NewFileDataStore(path string, size uint64) (DataStore, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &fileDataStore{
		file: file,
		size: size,
	}, nil
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
	readOffset := f.offset % f.ds.size
	readLength := f.size
	if readLength > f.ds.size-readOffset {
		readLength = f.ds.size - readOffset
	}
	if nRead, err := f.ds.file.ReadAt(b[:readLength], int64(readOffset)); err == io.EOF {
		for i := nRead; i < int(readLength); i++ {
			b[i] = 0
		}
	} else if err != nil {
		return 0, err
	}
	f.offset += readLength
	f.size -= readLength
	return int(readLength), nil
}

func (f *fileDataStoreReader) Close() error {
	return nil
}
