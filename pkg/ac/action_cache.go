package ac

import (
	"context"

	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

// ActionCache provides typed access to a Bazel Action Cache (AC).
type ActionCache interface {
	GetActionResult(ctx context.Context, digest *util.Digest) (*remoteexecution.ActionResult, error)
	PutActionResult(ctx context.Context, digest *util.Digest, result *remoteexecution.ActionResult) error
}
