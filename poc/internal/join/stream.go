package join

import (
	"context"
	"fmt"
	"io"

	"poc/internal/buffer"
	"poc/internal/spill"
)

const (
	minRead = 512 // at least buffer.MaxHeader, so a file larger than the buffer can be streamed
	maxRead = 1 << 20
)

// readSize returns the size of the buffer a pass streams spill files
// through: a sixteenth of the unit budget, within minRead and maxRead.
func (j *joiner) readSize() int64 {
	return min(max(j.budget/16, minRead), maxRead)
}

// streamSize returns the size of the buffer each streams the files of pins
// through: read bytes, or the size of their largest file if that is less,
// so that a pass over small files reserves no more than they take.
func streamSize(pins []pinned, read int64) int64 {
	var largest int64
	for _, p := range pins {
		for _, f := range p.files {
			largest = max(largest, f.Bytes)
		}
	}
	return min(read, largest)
}

// pinFor pins bufs for a pass that streams them through a buffer of at
// most read bytes, and reserves extra bytes besides the memory they hold,
// and the stream buffer's. If that doesn't fit next to the pinned buffers,
// it is reserved with them unpinned, so that the pool may spill them, and
// they are pinned again: streamed, their files need no more memory. It
// returns the bytes reserved.
func (j *joiner) pinFor(bufs []sided, extra, read int64) ([]pinned, int64, error) {
	pins, _, _, _ := pin(bufs)
	n := extra + streamSize(pins, read)
	if err := j.pool.Reserve(n); err != nil {
		unpin(pins)
		n = extra + read
		if err := j.pool.Reserve(n); err != nil {
			return nil, 0, err
		}
		pins, _, _, _ = pin(bufs)
		size := streamSize(pins, read)
		j.pool.Release(read - size)
		n = extra + size
	}
	return pins, n, nil
}

// each calls fn with the records of pins in order, a chunk of whole records
// at a time: each buffer's files, then its in-memory chunks. The files are
// streamed through a buffer of streamSize(pins, read) bytes, which pinFor
// reserved; a record larger than that grows the buffer, reserving the
// difference until each returns. A streamed chunk is valid only until fn
// returns.
func (j *joiner) each(pins []pinned, read int64, fn func(chunk) error) error {
	var buf []byte
	var grown int64
	defer func() { j.pool.Release(grown) }()
	for _, p := range pins {
		for _, f := range p.files {
			if buf == nil {
				buf = make([]byte, streamSize(pins, read))
			}
			err := j.readFile(f, &buf, &grown, func(data []byte) error {
				return fn(chunk{data: data, side: p.side, streamed: true})
			})
			if err != nil {
				if j.ctx.Err() != nil { // the run was deleted, its files with it
					return context.Cause(j.ctx)
				}
				return err
			}
		}
		for _, c := range p.mem {
			if err := j.ctx.Err(); err != nil {
				return context.Cause(j.ctx)
			}
			if err := fn(chunk{data: c, side: p.side}); err != nil {
				return err
			}
		}
	}
	return nil
}

// readFile calls fn with the records of f, as many whole records at a time
// as *buf holds. A record larger than *buf replaces it with a buffer of the
// record's size, whose extra bytes it reserves and adds to *grown.
func (j *joiner) readFile(f spill.File, buf *[]byte, grown *int64, fn func([]byte) error) error {
	r, err := j.disk.Open(f)
	if err != nil {
		return err
	}
	defer r.Close()
	n := 0          // bytes held in *buf
	left := f.Bytes // bytes not yet read
	for {
		if err := j.ctx.Err(); err != nil {
			return context.Cause(j.ctx)
		}
		b := *buf
		if m := min(int64(len(b)-n), left); m > 0 {
			if _, err := io.ReadFull(r, b[n:n+int(m)]); err != nil {
				return fmt.Errorf("read spill file %s: %w", f.Path, err)
			}
			n += int(m)
			left -= m
		}
		end := 0
		for {
			l, ok := buffer.RecordLen(b[end:n])
			if !ok || end+l > n {
				break
			}
			end += l
		}
		switch {
		case end > 0:
			if err := fn(b[:end]); err != nil {
				return err
			}
			n = copy(b, b[end:n])
		case n == 0 && left == 0:
			return nil
		case left == 0:
			return fmt.Errorf("spill file %s ends inside a record", f.Path)
		default:
			// Reading stopped with b full, so it holds the start of a
			// record longer than b.
			l, ok := buffer.RecordLen(b[:n])
			if !ok {
				return fmt.Errorf("spill file %s holds a corrupt record", f.Path)
			}
			if err := j.pool.Reserve(int64(l - len(b))); err != nil {
				return err
			}
			*grown += int64(l - len(b))
			bigger := make([]byte, l)
			copy(bigger, b[:n])
			*buf = bigger
		}
	}
}
