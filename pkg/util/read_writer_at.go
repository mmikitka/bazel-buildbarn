package util

import (
	"io"
)

type ReadWriterAt interface {
	io.ReaderAt
	io.WriterAt
}
