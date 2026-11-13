// Package api implements recon.v1.Reconciler: request validation, state
// checks and error mapping.
package api

import (
	"context"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/buffer"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/run"
	"poc/internal/spill"
)

// Server is the recon.v1.Reconciler service.
type Server struct {
	reconv1.UnimplementedReconcilerServer
	runs *run.Manager
}

// New returns a Server of runs.
func New(runs *run.Manager) *Server {
	return &Server{runs: runs}
}

func (s *Server) CreateRun(_ context.Context, req *reconv1.CreateRunRequest) (*reconv1.CreateRunResponse, error) {
	id, err := s.runs.Create(req.GetBucketCount())
	if err != nil {
		return nil, toStatus(err)
	}
	return &reconv1.CreateRunResponse{RunId: &reconv1.RunId{Value: id}}, nil
}

func (s *Server) SealRun(_ context.Context, req *reconv1.SealRunRequest) (*reconv1.SealRunResponse, error) {
	r, err := s.lookup(req.GetRunId())
	if err != nil {
		return nil, toStatus(err)
	}
	resp, err := r.Seal()
	if err != nil {
		return nil, toStatus(err)
	}
	return resp, nil
}

func (s *Server) DeleteRun(_ context.Context, req *reconv1.DeleteRunRequest) (*reconv1.DeleteRunResponse, error) {
	id, err := run.ParseID(req.GetRunId())
	if err == nil {
		err = s.runs.Delete(id)
	}
	if err != nil {
		return nil, toStatus(err)
	}
	return &reconv1.DeleteRunResponse{}, nil
}

func (s *Server) Join(req *reconv1.JoinRequest, stream grpc.ServerStreamingServer[reconv1.JoinResponse]) error {
	for _, st := range req.GetStates() {
		if st < reconv1.JoinState_JOIN_STATE_LEFT_ONLY || st > reconv1.JoinState_JOIN_STATE_BOTH {
			return toStatus(invalid("states holds %v, which is not a join state", st))
		}
	}
	r, err := s.lookup(req.GetRunId())
	if err != nil {
		return toStatus(err)
	}
	return toStatus(r.Join(stream.Context(), join.NewFilter(req.GetStates()), stream.Send))
}

func (s *Server) Ingest(stream grpc.ClientStreamingServer[reconv1.IngestRequest, reconv1.IngestResponse]) error {
	return toStatus(s.ingest(stream))
}

func (s *Server) ingest(stream grpc.ClientStreamingServer[reconv1.IngestRequest, reconv1.IngestResponse]) error {
	rx := receiver{stream: stream, ch: make(chan received, 1)}
	msg, err := rx.next(stream.Context())
	if errors.Is(err, io.EOF) {
		return stream.SendAndClose(&reconv1.IngestResponse{})
	}
	if err != nil {
		return err
	}

	// Everything about the first message is checked before a record is
	// accepted, so that rejecting it leaves the run as it was (§10).
	r, err := s.lookup(msg.GetRunId())
	if err != nil {
		return err
	}
	side, batch, err := batchOf(msg)
	if err != nil {
		return err
	}
	in, err := r.BeginIngest()
	if err != nil {
		return err
	}
	defer in.Abandon()
	ctx, cancel := r.WithContext(stream.Context())
	defer cancel()

	id := msg.GetRunId().GetValue()
	for {
		if err := in.Append(side, batch.GetRecords(), int64(proto.Size(msg))); err != nil {
			return err
		}
		msg, err = rx.next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if msg.GetRunId().GetValue() != id {
			return invalid("every message of an Ingest stream must name the same run")
		}
		if side, batch, err = batchOf(msg); err != nil {
			return err
		}
	}
	if err := in.Commit(); err != nil {
		return err
	}
	return stream.SendAndClose(&reconv1.IngestResponse{})
}

// batchOf returns the side and records of an IngestRequest.
func batchOf(msg *reconv1.IngestRequest) (buffer.Side, *reconv1.RecordBatch, error) {
	switch b := msg.GetBatch().(type) {
	case *reconv1.IngestRequest_Left:
		return buffer.Left, b.Left, nil
	case *reconv1.IngestRequest_Right:
		return buffer.Right, b.Right, nil
	default:
		return 0, nil, invalid("an IngestRequest must set left or right")
	}
}

type received struct {
	msg *reconv1.IngestRequest
	err error
}

// receiver receives from an Ingest stream in a goroutine, so that a
// handler waiting for the next message still sees its context end: grpc-go
// doesn't unblock Recv when it does. At most one Recv is in flight, and its
// result is buffered, so the goroutine never outlives the stream.
type receiver struct {
	stream  grpc.ClientStreamingServer[reconv1.IngestRequest, reconv1.IngestResponse]
	ch      chan received
	pending bool
}

// next returns the next message, or context.Cause(ctx) once ctx is done.
func (rx *receiver) next(ctx context.Context) (*reconv1.IngestRequest, error) {
	if !rx.pending {
		rx.pending = true
		go func() {
			msg, err := rx.stream.Recv()
			rx.ch <- received{msg, err}
		}()
	}
	select {
	case r := <-rx.ch:
		rx.pending = false
		return r.msg, r.err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

func (s *Server) lookup(id *reconv1.RunId) (*run.Run, error) {
	v, err := run.ParseID(id)
	if err != nil {
		return nil, err
	}
	return s.runs.Get(v)
}

func invalid(format string, args ...any) error {
	return &run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST, Detail: fmt.Sprintf(format, args...)}
}

// reasonCodes maps each ErrorReason to its gRPC code (§6.4).
var reasonCodes = map[reconv1.ErrorReason]codes.Code{
	reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST:    codes.InvalidArgument,
	reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND:      codes.NotFound,
	reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE:    codes.FailedPrecondition,
	reconv1.ErrorReason_ERROR_REASON_RUN_FAILED:         codes.FailedPrecondition,
	reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS: codes.FailedPrecondition,
	reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS:   codes.FailedPrecondition,
	reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED:   codes.ResourceExhausted,
	reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED:     codes.ResourceExhausted,
	reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS:      codes.ResourceExhausted,
	reconv1.ErrorReason_ERROR_REASON_HOT_KEY:            codes.ResourceExhausted,
}

// toStatus maps every failure an RPC returns to its status. A failure with
// an ErrorReason carries a ReconError detail. A cancelled stream keeps its
// context's code, which grpcserver turns into UNAVAILABLE at shutdown.
func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if re, ok := errors.AsType[*run.Error](err); ok {
		return withReason(&reconv1.ReconError{Reason: re.Reason, Detail: re.Detail})
	}
	if hk, ok := errors.AsType[*join.HotKeyError](err); ok {
		return withReason(&reconv1.ReconError{
			Reason: reconv1.ErrorReason_ERROR_REASON_HOT_KEY,
			Detail: hk.Error(),
			HotKey: &reconv1.HotKey{Key: hk.Key, LeftRecords: hk.Left, RightRecords: hk.Right},
		})
	}
	switch {
	case errors.Is(err, mem.ErrExhausted):
		return withReason(&reconv1.ReconError{Reason: reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED, Detail: err.Error()})
	case errors.Is(err, spill.ErrDiskExhausted):
		return withReason(&reconv1.ReconError{Reason: reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED, Detail: err.Error()})
	case errors.Is(err, run.ErrAborted):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, join.ErrControlTotals):
		return status.Error(codes.Internal, err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func withReason(re *reconv1.ReconError) error {
	code := reasonCodes[re.GetReason()]
	st, err := status.New(code, re.GetDetail()).WithDetails(re)
	if err != nil {
		return status.Error(code, re.GetDetail())
	}
	return st.Err()
}
