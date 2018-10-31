package builder

import (
	"context"

	"github.com/EdSchouten/bazel-buildbarn/pkg/ac"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

type cachingBuildExecutor struct {
	base        BuildExecutor
	actionCache ac.ActionCache
}

// NewCachingBuildExecutor creates an adapter for BuildExecutor that
// stores action results in the Action Cache (AC) if they may be cached.
func NewCachingBuildExecutor(base BuildExecutor, actionCache ac.ActionCache) BuildExecutor {
	return &cachingBuildExecutor{
		base:        base,
		actionCache: actionCache,
	}
}

func (be *cachingBuildExecutor) Execute(ctx context.Context, request *remoteexecution.ExecuteRequest) (*remoteexecution.ExecuteResponse, bool) {
	response, mayBeCached := be.base.Execute(ctx, request)
	if mayBeCached {
		digest, err := util.NewDigest(request.InstanceName, request.ActionDigest)
		if err != nil {
			return convertErrorToExecuteResponse(err), false
		}
		if err := be.actionCache.PutActionResult(ctx, digest, response.Result); err != nil {
			return convertErrorToExecuteResponse(err), false
		}
	}
	return response, mayBeCached
}
