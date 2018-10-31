package blobstore

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"

	"golang.org/x/net/context/ctxhttp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type remoteBlobAccess struct {
	address string
	prefix  string
}

func convertHTTPUnexpectedStatus(resp *http.Response) error {
	return status.Errorf(codes.Unknown, "Unexpected status code from remote cache: %d - %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}

// NewRemoteBlobAccess for use of HTTP/1.1 cache backend.
//
// See: https://docs.bazel.build/versions/master/remote-caching.html#http-caching-protocol
func NewRemoteBlobAccess(address, prefix string) BlobAccess {
	return &remoteBlobAccess{
		address: address,
		prefix:  prefix,
	}
}

func (ba *remoteBlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, hex.EncodeToString(digest.GetHash()))
	resp, err := ctxhttp.Get(ctx, http.DefaultClient, url)
	if err != nil {
		fmt.Printf("Error getting digest. %s\n", err)
		return util.NewErrorReader(err)
	}

	switch resp.StatusCode {
	case http.StatusNotFound:
		return util.NewErrorReader(status.Error(codes.NotFound, url))
	case http.StatusOK:
		return resp.Body
	default:
		return util.NewErrorReader(convertHTTPUnexpectedStatus(resp))
	}
}

func (ba *remoteBlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, hex.EncodeToString(digest.GetHash()))
	req, err := http.NewRequest(http.MethodPut, url, r)
	if err != nil {
		r.Close()
		return err
	}
	req.ContentLength = sizeBytes
	_, err = ctxhttp.Do(ctx, http.DefaultClient, req)
	return err
}

func (ba *remoteBlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	return status.Error(codes.Unimplemented, "Bazel HTTP caching protocol does not support object deletion")
}

func (ba *remoteBlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	var missing []*util.Digest
	for _, digest := range digests {
		url := fmt.Sprintf("%s/%s/%s", ba.address, ba.prefix, hex.EncodeToString(digest.GetHash()))
		resp, err := ctxhttp.Head(ctx, http.DefaultClient, url)
		if err != nil {
			return nil, err
		}

		switch resp.StatusCode {
		case http.StatusNotFound:
			missing = append(missing, digest)
		case http.StatusOK:
			continue
		default:
			return nil, convertHTTPUnexpectedStatus(resp)
		}
	}

	return missing, nil
}
