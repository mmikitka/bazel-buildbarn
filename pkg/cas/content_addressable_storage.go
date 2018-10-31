package cas

import (
	"context"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// ContentAddressableStorage provides typed access to a Bazel Content
// Addressable Storage (CAS).
type ContentAddressableStorage interface {
	GetAction(ctx context.Context, digest *util.Digest) (*remoteexecution.Action, error)
	GetCommand(ctx context.Context, digest *util.Digest) (*remoteexecution.Command, error)
	GetDirectory(ctx context.Context, digest *util.Digest) (*remoteexecution.Directory, error)
	GetFile(ctx context.Context, digest *util.Digest, outputPath string, isExecutable bool) error
	PutFile(ctx context.Context, path string, parentDigest *util.Digest) (*util.Digest, bool, error)
	PutTree(ctx context.Context, tree *remoteexecution.Tree, parentDigest *util.Digest) (*util.Digest, error)
}
