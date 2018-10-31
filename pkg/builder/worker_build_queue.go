package builder

import (
	"log"
	"sync"

	"github.com/EdSchouten/bazel-buildbarn/pkg/proto/scheduler"
	"github.com/EdSchouten/bazel-buildbarn/pkg/util"
	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/ptypes"
	"github.com/satori/go.uuid"

	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workerBuildJob struct {
	name             string
	actionDigest     *remoteexecution.Digest
	deduplicationKey string
	executeRequest   remoteexecution.ExecuteRequest

	stage                   remoteexecution.ExecuteOperationMetadata_Stage
	executeResponse         *remoteexecution.ExecuteResponse
	executeTransitionWakeup *sync.Cond
}

func (job *workerBuildJob) waitExecution(out remoteexecution.Execution_ExecuteServer) error {
	for {
		// Send current state.
		metadata, err := ptypes.MarshalAny(&remoteexecution.ExecuteOperationMetadata{
			Stage:        job.stage,
			ActionDigest: job.actionDigest,
		})
		if err != nil {
			log.Fatal("Failed to marshal execute operation metadata: ", err)
		}
		operation := &longrunning.Operation{
			Name:     job.name,
			Metadata: metadata,
		}
		if job.executeResponse != nil {
			operation.Done = true
			response, err := ptypes.MarshalAny(job.executeResponse)
			if err != nil {
				log.Fatal("Failed to marshal execute response: ", err)
			}
			operation.Result = &longrunning.Operation_Response{Response: response}
		}
		if err := out.Send(operation); err != nil {
			return err
		}

		// Wait for state transition.
		// TODO(edsch): Should take a context.
		// TODO(edsch): Should wake up periodically.
		if job.executeResponse != nil {
			return nil
		}
		job.executeTransitionWakeup.Wait()
	}
}

type workerBuildQueue struct {
	deduplicationKeyFormat util.DigestKeyFormat
	jobsPendingMax         uint

	jobsLock                   sync.Mutex
	jobsNameMap                map[string]*workerBuildJob
	jobsDeduplicationMap       map[string]*workerBuildJob
	jobsPending                []*workerBuildJob
	jobsPendingInsertionWakeup *sync.Cond
}

// NewWorkerBuildQueue creates an execution server that places execution
// requests in a queue. These execution requests may be extracted by
// workers.
func NewWorkerBuildQueue(deduplicationKeyFormat util.DigestKeyFormat, jobsPendingMax uint) (remoteexecution.ExecutionServer, scheduler.SchedulerServer) {
	bq := &workerBuildQueue{
		deduplicationKeyFormat: deduplicationKeyFormat,
		jobsPendingMax:         jobsPendingMax,

		jobsNameMap:          map[string]*workerBuildJob{},
		jobsDeduplicationMap: map[string]*workerBuildJob{},
	}
	bq.jobsPendingInsertionWakeup = sync.NewCond(&bq.jobsLock)
	return bq, bq
}

func (bq *workerBuildQueue) Execute(in *remoteexecution.ExecuteRequest, out remoteexecution.Execution_ExecuteServer) error {
	digest, err := util.NewDigest(in.InstanceName, in.ActionDigest)
	if err != nil {
		return err
	}
	deduplicationKey := digest.GetKey(bq.deduplicationKeyFormat)

	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	job, ok := bq.jobsDeduplicationMap[deduplicationKey]
	if !ok {
		// TODO(edsch): Maybe let the number of workers influence this?
		if uint(len(bq.jobsPending)) >= bq.jobsPendingMax {
			return status.Errorf(codes.Unavailable, "Too many jobs pending")
		}

		job = &workerBuildJob{
			name:             uuid.NewV4().String(),
			actionDigest:     in.ActionDigest,
			deduplicationKey: deduplicationKey,
			executeRequest:   *in,
			stage:            remoteexecution.ExecuteOperationMetadata_QUEUED,
			executeTransitionWakeup: sync.NewCond(&bq.jobsLock),
		}
		bq.jobsNameMap[job.name] = job
		bq.jobsDeduplicationMap[deduplicationKey] = job
		bq.jobsPending = append(bq.jobsPending, job)
		bq.jobsPendingInsertionWakeup.Signal()
	}
	return job.waitExecution(out)
}

func (bq *workerBuildQueue) WaitExecution(in *remoteexecution.WaitExecutionRequest, out remoteexecution.Execution_WaitExecutionServer) error {
	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	job, ok := bq.jobsNameMap[in.Name]
	if !ok {
		return status.Errorf(codes.NotFound, "Build job with name %s not found", in.Name)
	}
	return job.waitExecution(out)
}

func executeOnWorker(stream scheduler.Scheduler_GetWorkServer, request *remoteexecution.ExecuteRequest) *remoteexecution.ExecuteResponse {
	// TODO(edsch): Any way we can set a timeout here?
	if err := stream.Send(request); err != nil {
		return convertErrorToExecuteResponse(err)
	}
	response, err := stream.Recv()
	if err != nil {
		return convertErrorToExecuteResponse(err)
	}
	return response
}

func (bq *workerBuildQueue) GetWork(stream scheduler.Scheduler_GetWorkServer) error {
	bq.jobsLock.Lock()
	defer bq.jobsLock.Unlock()

	// TODO(edsch): Purge jobs from the jobsNameMap after some amount of time.
	for {
		// Wait for jobs to appear.
		// TODO(edsch): sync.Cond.WaitWithContext() would be helpful here.
		for len(bq.jobsPending) == 0 {
			bq.jobsPendingInsertionWakeup.Wait()
		}
		if err := stream.Context().Err(); err != nil {
			bq.jobsPendingInsertionWakeup.Signal()
			return err
		}

		// Extract job from queue.
		job := bq.jobsPending[0]
		bq.jobsPending = bq.jobsPending[1:]
		job.stage = remoteexecution.ExecuteOperationMetadata_EXECUTING

		// Perform execution of the job.
		bq.jobsLock.Unlock()
		executeResponse := executeOnWorker(stream, &job.executeRequest)
		bq.jobsLock.Lock()

		// Mark completion.
		delete(bq.jobsDeduplicationMap, job.deduplicationKey)
		job.stage = remoteexecution.ExecuteOperationMetadata_COMPLETED
		job.executeResponse = executeResponse
		job.executeTransitionWakeup.Broadcast()
	}
}
