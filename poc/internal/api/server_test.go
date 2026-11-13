package api

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/join"
	"poc/internal/mem"
	"poc/internal/run"
)

func TestToStatus(t *testing.T) {
	cases := []struct {
		err    error
		code   codes.Code
		reason reconv1.ErrorReason // UNSPECIFIED: no ReconError
	}{
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST}, codes.InvalidArgument, reconv1.ErrorReason_ERROR_REASON_INVALID_REQUEST},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND}, codes.NotFound, reconv1.ErrorReason_ERROR_REASON_RUN_NOT_FOUND},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE}, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_RUN_WRONG_STATE},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_RUN_FAILED}, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_RUN_FAILED},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS}, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_INGEST_IN_PROGRESS},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS}, codes.FailedPrecondition, reconv1.ErrorReason_ERROR_REASON_JOIN_IN_PROGRESS},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED}, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_DISK_EXHAUSTED},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS}, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_TOO_MANY_RUNS},
		{&run.Error{Reason: reconv1.ErrorReason_ERROR_REASON_HOT_KEY}, codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_HOT_KEY},
		{fmt.Errorf("append: %w", mem.ErrExhausted), codes.ResourceExhausted, reconv1.ErrorReason_ERROR_REASON_MEMORY_EXHAUSTED},
		{run.ErrAborted, codes.Aborted, 0},
		{fmt.Errorf("%w: off by one", join.ErrControlTotals), codes.Internal, 0},
		{context.Canceled, codes.Canceled, 0},
		{context.DeadlineExceeded, codes.DeadlineExceeded, 0},
		{status.Error(codes.Unavailable, "transport closed"), codes.Unavailable, 0},
		{errors.New("disk on fire"), codes.Internal, 0},
	}
	for _, c := range cases {
		st := status.Convert(toStatus(c.err))
		if st.Code() != c.code {
			t.Errorf("toStatus(%v): code %v, want %v", c.err, st.Code(), c.code)
		}
		var got reconv1.ErrorReason
		for _, d := range st.Details() {
			if re, ok := d.(*reconv1.ReconError); ok {
				got = re.GetReason()
			}
		}
		if got != c.reason {
			t.Errorf("toStatus(%v): reason %v, want %v", c.err, got, c.reason)
		}
	}
	if toStatus(nil) != nil {
		t.Error("toStatus(nil) != nil")
	}
}

func TestToStatus_HOT_KEY_names_the_key(t *testing.T) {
	err := fmt.Errorf("join: %w", &join.HotKeyError{Key: []byte("N/A"), Left: 3, Right: 4})
	st := status.Convert(toStatus(err))
	if st.Code() != codes.ResourceExhausted {
		t.Errorf("code %v, want ResourceExhausted", st.Code())
	}
	want := &reconv1.ReconError{
		Reason: reconv1.ErrorReason_ERROR_REASON_HOT_KEY,
		HotKey: &reconv1.HotKey{Key: []byte("N/A"), LeftRecords: 3, RightRecords: 4},
	}
	if d := st.Details(); len(d) != 1 || !proto.Equal(d[0].(proto.Message), want) {
		t.Errorf("details %v, want %v", d, want)
	}
}
