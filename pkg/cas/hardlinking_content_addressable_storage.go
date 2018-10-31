package cas

import (
	"context"
	"math/rand"
	"os"
	"path"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
)

type hardlinkingContentAddressableStorage struct {
	ContentAddressableStorage

	digestKeyFormat util.DigestKeyFormat
	path            string
	maxFiles        int
	maxSize         int64

	filesPresentList      []string
	filesPresentSize      map[string]int64
	filesPresentTotalSize int64
}

// NewHardlinkingContentAddressableStorage is an adapter for
// ContentAddressableStorage that stores files in an internal directory.
// Only after successfully downloading files, they are hardlinked to the
// target location. This reduces the amount of network traffic needed.
func NewHardlinkingContentAddressableStorage(base ContentAddressableStorage, digestKeyFormat util.DigestKeyFormat, path string, maxFiles int, maxSize int64) ContentAddressableStorage {
	return &hardlinkingContentAddressableStorage{
		ContentAddressableStorage: base,

		digestKeyFormat: digestKeyFormat,
		path:            path,
		maxFiles:        maxFiles,
		maxSize:         maxSize,

		filesPresentSize: map[string]int64{},
	}
}

func (cas *hardlinkingContentAddressableStorage) makeSpace(size int64) error {
	for len(cas.filesPresentList) > 0 && (len(cas.filesPresentList) >= cas.maxFiles || cas.filesPresentTotalSize+size > cas.maxSize) {
		// Remove random file from disk.
		idx := rand.Intn(len(cas.filesPresentList))
		key := cas.filesPresentList[idx]
		if err := os.Remove(path.Join(cas.path, key)); err != nil {
			return err
		}

		// Remove file from bookkeeping.
		cas.filesPresentTotalSize -= cas.filesPresentSize[key]
		delete(cas.filesPresentSize, key)
		last := len(cas.filesPresentList) - 1
		cas.filesPresentList[idx] = cas.filesPresentList[last]
		cas.filesPresentList = cas.filesPresentList[:last]
	}
	return nil
}

func (cas *hardlinkingContentAddressableStorage) GetFile(ctx context.Context, digest *util.Digest, outputPath string, isExecutable bool) error {
	key := digest.GetKey(cas.digestKeyFormat)
	if isExecutable {
		key += "+x"
	} else {
		key += "-x"
	}

	cachePath := path.Join(cas.path, key)
	if _, ok := cas.filesPresentSize[key]; !ok {
		sizeBytes := digest.GetSizeBytes()
		if err := cas.makeSpace(sizeBytes); err != nil {
			return err
		}
		if err := cas.ContentAddressableStorage.GetFile(ctx, digest, cachePath, isExecutable); err != nil {
			return err
		}
		cas.filesPresentList = append(cas.filesPresentList, key)
		cas.filesPresentSize[key] = sizeBytes
		cas.filesPresentTotalSize += sizeBytes
	}
	return os.Link(cachePath, outputPath)
}
