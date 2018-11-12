package circular

import (
	"io"
)

type DataStore interface {
	Put(b []byte, offset uint64) error
	Get(offset uint64, size int64) io.ReadCloser
}