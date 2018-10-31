package blobstore

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	"github.com/go-redis/redis"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type redisBlobAccess struct {
	redisClient   *redis.Client
	blobKeyFormat util.DigestKeyFormat
}

// NewRedisBlobAccess creates a BlobAccess that uses Redis as its
// backing store.
func NewRedisBlobAccess(redisClient *redis.Client, blobKeyFormat util.DigestKeyFormat) BlobAccess {
	return &redisBlobAccess{
		redisClient:   redisClient,
		blobKeyFormat: blobKeyFormat,
	}
}

func (ba *redisBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	if err := ctx.Err(); err != nil {
		return util.NewErrorReader(err)
	}
	value, err := ba.redisClient.Get(digest.GetKey(ba.blobKeyFormat)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return util.NewErrorReader(status.Errorf(codes.NotFound, err.Error()))
		}
		return util.NewErrorReader(err)
	}
	return ioutil.NopCloser(bytes.NewBuffer(value))
}

func (ba *redisBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	if err := ctx.Err(); err != nil {
		r.Close()
		return err
	}
	value, err := ioutil.ReadAll(r)
	r.Close()
	if err != nil {
		return err
	}
	return ba.redisClient.Set(digest.GetKey(ba.blobKeyFormat), value, 0).Err()
}

func (ba *redisBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	return ba.redisClient.Del(digest.GetKey(ba.blobKeyFormat)).Err()
}

func (ba *redisBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(digests) == 0 {
		return nil, nil
	}

	// Execute "EXISTS" requests all in a single pipeline.
	pipeline := ba.redisClient.Pipeline()
	var cmds []*redis.IntCmd
	for _, digest := range digests {
		cmds = append(cmds, pipeline.Exists(digest.GetKey(ba.blobKeyFormat)))
	}
	_, err := pipeline.Exec()
	if err != nil {
		return nil, err
	}

	var missing []*util.Digest
	for i, cmd := range cmds {
		if cmd.Val() == 0 {
			missing = append(missing, digests[i])
		}
	}
	return missing, nil
}
