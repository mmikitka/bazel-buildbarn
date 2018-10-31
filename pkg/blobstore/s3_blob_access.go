package blobstore

import (
	"context"
	"io"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertS3Error(err error) error {
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case s3.ErrCodeNoSuchKey, "NotFound":
				err = status.Errorf(codes.NotFound, awsErr.Message())
			}
		}
	}
	return err
}

type s3BlobAccess struct {
	s3            *s3.S3
	uploader      *s3manager.Uploader
	bucketName    *string
	blobKeyFormat util.DigestKeyFormat
	keyPrefix     string
}

// NewS3BlobAccess creates a BlobAccess that uses an S3 bucket as its backing
// store.
func NewS3BlobAccess(s3 *s3.S3, uploader *s3manager.Uploader, bucketName *string, keyPrefix string, blobKeyFormat util.DigestKeyFormat) BlobAccess {
	return &s3BlobAccess{
		s3:            s3,
		uploader:      uploader,
		bucketName:    bucketName,
		blobKeyFormat: blobKeyFormat,
		keyPrefix:     keyPrefix,
	}
}

func (ba *s3BlobAccess) Get(ctx context.Context, digest *util.Digest) io.ReadCloser {
	result, err := ba.s3.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: ba.bucketName,
		Key:    ba.getKey(digest),
	})
	if err != nil {
		return util.NewErrorReader(convertS3Error(err))
	}
	return result.Body
}

func (ba *s3BlobAccess) Put(ctx context.Context, digest *util.Digest, sizeBytes int64, r io.ReadCloser) error {
	defer r.Close()
	_, err := ba.uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket: ba.bucketName,
		Key:    ba.getKey(digest),
		Body:   r,
	})
	return convertS3Error(err)
}

func (ba *s3BlobAccess) Delete(ctx context.Context, digest *util.Digest) error {
	_, err := ba.s3.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: ba.bucketName,
		Key:    ba.getKey(digest),
	})
	return convertS3Error(err)
}

func (ba *s3BlobAccess) FindMissing(ctx context.Context, digests []*util.Digest) ([]*util.Digest, error) {
	var missing []*util.Digest
	for _, digest := range digests {
		_, err := ba.s3.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: ba.bucketName,
			Key:    ba.getKey(digest),
		})
		if err != nil {
			err = convertS3Error(err)
			if status.Code(err) == codes.NotFound {
				missing = append(missing, digest)
			} else {
				return nil, err
			}
		}
	}
	return missing, nil
}

func (ba *s3BlobAccess) getKey(digest *util.Digest) *string {
	s := ba.keyPrefix + digest.GetKey(ba.blobKeyFormat)
	return &s
}
