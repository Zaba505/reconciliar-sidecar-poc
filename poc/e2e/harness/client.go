package harness

import (
	"context"
	"errors"
	"io"
	"iter"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
)

// BatchBytes is the size Ingest aims for in each message (§6.1).
const BatchBytes = 1 << 20

// Side is the side of a run a record belongs to.
type Side int

const (
	Left Side = iota + 1
	Right
)

// Record is one record for Ingest.
type Record struct {
	Side       Side
	Key, Value []byte
}

// Ingest uploads records over one Ingest stream for run, batching
// consecutive records of the same side into messages of about BatchBytes.
// It returns the stream's final status: nil means the records committed.
func Ingest(ctx context.Context, c reconv1.ReconcilerClient, run *reconv1.RunId, records iter.Seq[Record]) error {
	stream, err := c.Ingest(ctx)
	if err != nil {
		return err
	}
	var (
		side  Side
		batch []*reconv1.Record
		size  int
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		req := &reconv1.IngestRequest{RunId: run}
		rb := &reconv1.RecordBatch{Records: batch}
		if side == Left {
			req.Batch = &reconv1.IngestRequest_Left{Left: rb}
		} else {
			req.Batch = &reconv1.IngestRequest_Right{Right: rb}
		}
		batch, size = nil, 0
		return stream.Send(req)
	}
	for r := range records {
		rec := &reconv1.Record{Key: r.Key, Value: r.Value}
		n := proto.Size(rec) + 4 // field tag and length prefix in the batch
		if r.Side != side || size+n > BatchBytes {
			if err := flush(); err != nil {
				return sendErr(stream, err)
			}
			side = r.Side
		}
		batch = append(batch, rec)
		size += n
	}
	if err := flush(); err != nil {
		return sendErr(stream, err)
	}
	_, err = stream.CloseAndRecv()
	return err
}

// sendErr returns the stream's status when Send failed with io.EOF, which
// grpc-go returns when the server has already ended the stream.
func sendErr(stream grpc.ClientStreamingClient[reconv1.IngestRequest, reconv1.IngestResponse], err error) error {
	if errors.Is(err, io.EOF) {
		_, err = stream.CloseAndRecv()
	}
	return err
}

// Join streams the results of req to fn, one at a time. It returns the
// stream's final status, nil when the join completed, or fn's error, which
// cancels the stream.
func Join(ctx context.Context, c reconv1.ReconcilerClient, req *reconv1.JoinRequest, fn func(*reconv1.JoinResult) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.Join(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		for _, r := range resp.GetResults() {
			if err := fn(r); err != nil {
				return err
			}
		}
	}
}

// JoinAll collects every result of req. The results are complete only when
// the error is nil.
func JoinAll(ctx context.Context, c reconv1.ReconcilerClient, req *reconv1.JoinRequest) ([]*reconv1.JoinResult, error) {
	var results []*reconv1.JoinResult
	err := Join(ctx, c, req, func(r *reconv1.JoinResult) error {
		results = append(results, r)
		return nil
	})
	return results, err
}

// ReconError returns the ReconError attached to err's status, or nil.
func ReconError(err error) *reconv1.ReconError {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if re, ok := d.(*reconv1.ReconError); ok {
			return re
		}
	}
	return nil
}
