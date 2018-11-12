package circular

type StateStore interface {
	Get() (uint64, uint64, error)
	Put(readCursor uint64, writeCursor uint64) error
}
