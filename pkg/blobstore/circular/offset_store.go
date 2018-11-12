package circular

import (
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
)

type OffsetStore interface {
	Get(digest *util.Digest, minOffset uint64, maxOffset uint64) (uint64, bool, error)
	Put(digest *util.Digest, minOffset uint64, newOffset uint64) error
}
