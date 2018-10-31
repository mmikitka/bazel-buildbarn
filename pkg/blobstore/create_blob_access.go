package blobstore

import (
	"errors"
	"fmt"
	"io/ioutil"

	pb "github.com/EdSchouten/bazel-buildbarn/pkg/proto/blobstore"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-redis/redis"
	"github.com/golang/protobuf/proto"
)

// CreateBlobAccessObjectsFromConfig creates a pair of BlobAccess
// objects for the Content Addressable Storage and Action cache based on
// a configuration file.
func CreateBlobAccessObjectsFromConfig(configurationFile string) (BlobAccess, BlobAccess, error) {
	data, err := ioutil.ReadFile(configurationFile)
	if err != nil {
		return nil, nil, err
	}
	var config pb.BlobstoreConfiguration
	if err := proto.UnmarshalText(string(data), &config); err != nil {
		return nil, nil, err
	}

	// Create two stores based on definitions in configuration.
	contentAddressableStorage, err := createBlobAccess(config.ContentAddressableStorage, "cas", util.DigestKeyWithoutInstance)
	if err != nil {
		return nil, nil, err
	}
	actionCache, err := createBlobAccess(config.ActionCache, "ac", util.DigestKeyWithInstance)
	if err != nil {
		return nil, nil, err
	}

	// Stack a mandatory layer on top to protect against data corruption.
	contentAddressableStorage = NewMetricsBlobAccess(
		NewMerkleBlobAccess(contentAddressableStorage),
		"cas_merkle")
	return contentAddressableStorage, actionCache, nil
}

func createBlobAccess(config *pb.BlobAccessConfiguration, storageType string, digestKeyFormat util.DigestKeyFormat) (BlobAccess, error) {
	var implementation BlobAccess
	var backendType string
	switch backend := config.Backend.(type) {
	case *pb.BlobAccessConfiguration_Redis:
		backendType = "redis"
		implementation = NewRedisBlobAccess(
			redis.NewClient(
				&redis.Options{
					Addr: backend.Redis.Endpoint,
					DB:   int(backend.Redis.Db),
				}),
			digestKeyFormat)
	case *pb.BlobAccessConfiguration_Remote:
		backendType = "remote"
		implementation = NewRemoteBlobAccess(backend.Remote.Address, storageType)
	case *pb.BlobAccessConfiguration_S3:
		backendType = "s3"
		cfg := aws.Config{
			Endpoint:         &backend.S3.Endpoint,
			Region:           &backend.S3.Region,
			DisableSSL:       &backend.S3.DisableSsl,
			S3ForcePathStyle: aws.Bool(true),
		}
		// If AccessKeyId isn't specified, allow AWS to search for credentials.
		// In AWS EC2, this search will include the instance IAM Role.
		if backend.S3.AccessKeyId != "" {
			cfg.Credentials = credentials.NewStaticCredentials(backend.S3.AccessKeyId, backend.S3.SecretAccessKey, "")
		}
		session := session.New(&cfg)
		s3 := s3.New(session)
		// Set the uploader concurrency to 1 to drastically reduce memory usage.
		// TODO(edsch): Maybe the concurrency can be left alone for this process?
		uploader := s3manager.NewUploader(session)
		uploader.Concurrency = 1
		implementation = NewS3BlobAccess(
			s3,
			uploader,
			&backend.S3.Bucket,
			backend.S3.KeyPrefix,
			digestKeyFormat)
	case *pb.BlobAccessConfiguration_SizeDistinguishing:
		backendType = "size_distinguishing"
		small, err := createBlobAccess(backend.SizeDistinguishing.Small, storageType, digestKeyFormat)
		if err != nil {
			return nil, err
		}
		large, err := createBlobAccess(backend.SizeDistinguishing.Large, storageType, digestKeyFormat)
		if err != nil {
			return nil, err
		}
		implementation = NewSizeDistinguishingBlobAccess(small, large, backend.SizeDistinguishing.CutoffSizeBytes)
	default:
		return nil, errors.New("Configuration did not contain a backend")
	}
	return NewMetricsBlobAccess(implementation, fmt.Sprintf("%s_%s", storageType, backendType)), nil
}
