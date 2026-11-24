package join

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/z5labs/humus"

	"poc/internal/buffer"
	"poc/internal/mem"
	"poc/internal/merge"
	"poc/internal/spill"
)

// Files is where a merge writes a run's files: the run's directory, whose
// deletion removes them.
type Files interface {
	// Format is the format of the files a merge writes.
	Format() spill.Format
	// Create reserves bytes on disk and creates a new run file, which buf
	// is written through, in the format of enc.
	Create(bytes int64, buf []byte, enc *spill.Encoder) (*spill.Writer, error)
	// Unreserve gives back bytes Create reserved: all of them for a file
	// that wasn't written, which the Writer removed, or those a file didn't
	// take.
	Unreserve(bytes int64)
	// Remove removes files and gives back their bytes.
	Remove(files []*spill.File)
}

// MergeStats describe what Compact did.
type MergeStats struct {
	Merges  int   // files written
	Bytes   int64 // in them
	Skipped bool  // merges were left undone for want of memory
}

// Compact merges the run files of buf, which is sealed and in files only,
// until a Join could read them all with one worker and read buffers of
// mergeReadBuf bytes, or of a block, if that is larger: the intermediate
// merge pass of an external sort. It
// merges the files next to each other whose bytes are fewest, so the order
// of the files stays the ingest order.
//
// A merge the pool has no memory for is left undone: Compact returns
// without an error, and the Join reads more files.
//
// Most runs need no merge: the files of 10 GiB under a 128 MiB budget are
// read by four workers at once. Compact is for data that outgrows the
// budget hundreds of times, which would otherwise join slowly or fail for
// want of read buffers.
func Compact(ctx context.Context, pool *mem.Pool, fsys humus.FS, buf *buffer.Buffer, files Files, opts Options) (MergeStats, error) {
	var st MergeStats
	maxMatch := MaxMatch(pool.Limit(), opts)
	for {
		runs := buf.Files()
		budget := readBudget(pool, 1, maxMatch, opts.OutputBatchBytes)
		most := max(int(budget/int64(mergeBuffer(runs))), 2)
		if len(runs) <= most {
			return st, nil
		}
		// Merge the window of fan files whose bytes are fewest that brings
		// the count to most, or as many as the budget reads at once.
		fan := min(len(runs)-most+1, most)
		best, bestSize := 0, int64(-1)
		var size int64
		for i, r := range runs {
			size += r.Bytes
			if i >= fan {
				size -= runs[i-fan].Bytes
			}
			if i >= fan-1 && (bestSize < 0 || size < bestSize) {
				best, bestSize = i-fan+1, size
			}
		}
		f, err := mergeFiles(ctx, pool, fsys, files, runs[best:best+fan], budget)
		if errors.Is(err, mem.ErrExhausted) {
			// Merging only makes the Join faster, which reads the files as
			// they are, with smaller buffers or fewer workers.
			st.Skipped = true
			return st, nil
		}
		if err != nil {
			return st, err
		}
		buf.Replace(best, best+fan, f)
		files.Remove(runs[best : best+fan])
		st.Merges++
		st.Bytes += f.Bytes
	}
}

// mergeBuffer returns the read buffer Compact merges files down to.
func mergeBuffer(files []*spill.File) int { return max(mergeReadBuf, spill.MinBuffer(files)) }

// mergeFiles writes the records of runs to one new file, with read buffers
// that take at most budget bytes.
func mergeFiles(ctx context.Context, pool *mem.Pool, fsys humus.FS, files Files, runs []*spill.File, budget int64) (*spill.File, error) {
	fan := len(runs)
	bufSize := int(min(budget/int64(fan), maxReadBuf)) &^ (spill.Align - 1)
	if least := max(minReadBuf, spill.MinBuffer(runs)); bufSize < least {
		return nil, fmt.Errorf("%w: a merge of %d run files needs read buffers of %d bytes each, and may take %d bytes",
			mem.ErrExhausted, fan, least, budget)
	}
	format := files.Format()
	writeSize := buffer.BlockSize(pool.Limit()) &^ (spill.Align - 1)
	reserved := int64(fan)*int64(bufSize) + merge.Bytes(fan) + int64(fan)*sourceBytes + int64(writeSize) + spill.Align +
		spill.DecoderBytes(runs) + format.EncoderBytes()
	if err := pool.Reserve(reserved); err != nil {
		return nil, err
	}
	defer func() { pool.Release(reserved) }()

	var raw int64
	for _, r := range runs {
		raw += r.Raw
	}
	bytes := format.MaxBytes(raw)
	slab := make([]byte, fan*bufSize)
	srcs := make([]merge.Source, fan)
	grow := func(n int) ([]byte, error) {
		if err := pool.Reserve(int64(n)); err != nil {
			return nil, err
		}
		reserved += int64(n)
		return make([]byte, n), nil
	}
	var opened []*os.File
	dec := spill.NewDecoder(runs)
	defer func() {
		for _, f := range opened {
			_ = f.Close()
		}
		dec.Close()
	}()
	for i, r := range runs {
		f, err := spill.Open(fsys, r)
		if err != nil {
			return nil, err
		}
		opened = append(opened, f)
		c := spill.NewFileCursor(f, r, uint64(i), slab[i*bufSize:(i+1)*bufSize:(i+1)*bufSize], grow, dec)
		if err := c.Reset(0, r.Bytes); err != nil {
			return nil, err
		}
		srcs[i] = c
	}
	m := merge.New(srcs)
	m.Init()

	w, err := files.Create(bytes, spill.AlignedBuffer(writeSize), spill.NewEncoder(format))
	if err != nil {
		return nil, err
	}
	if err := spill.Copy(ctx, w, m); err != nil {
		w.Abort()
		files.Unreserve(bytes)
		return nil, err
	}
	f, err := w.Finish()
	if err != nil {
		files.Unreserve(bytes)
		return nil, err
	}
	files.Unreserve(bytes - f.Bytes)
	if f.Raw != raw {
		files.Remove([]*spill.File{f})
		return nil, errors.New("join: a merge wrote a different number of bytes than it read")
	}
	return f, nil
}
