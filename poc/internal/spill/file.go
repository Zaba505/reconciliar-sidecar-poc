// Package spill holds a run's records on the data directory as sorted run
// files (§8.3).
//
// A spill writes everything a run holds in memory as one new run file, in
// (hash, key, side, ingest) order. A run file is records back to back,
// encoded as package record encodes them, with no header and no index; or,
// in a Format with a codec, those records in blocks, each compressed on
// its own (see block.go). Its table of fences, the offset at which each
// fence's records start, stays in memory with the File.
//
// Run files are written once, sequentially, through one aligned buffer,
// with direct I/O where the filesystem supports it, so that a spill of a
// whole budget doesn't fill the page cache with dirty pages. They are read
// with pread through buffers the join reserves, with the kernel's readahead
// off. Nothing is synced: runs don't survive a restart, and the data
// directory is wiped at startup.
package spill

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/z5labs/humus"

	"poc/internal/merge"
	"poc/internal/record"
)

// File is one run file.
type File struct {
	Path    string
	Format  Format
	Bytes   int64 // of the file
	Raw     int64 // of its records: Bytes, unless it holds blocks
	Records int64
	// Fences[i] is the offset of the first record of fence i, or of the
	// block that starts with it, and Fences[record.Fences] the size of the
	// file.
	Fences [record.Fences + 1]int64
	// A file of blocks keeps the bytes of each fence's records too: in
	// rawSizes if every fence holds less than 4 GiB, as all but a hot key's
	// do, and in rawFences, the bytes before each fence, if not. In a file
	// without blocks, its Fences are both.
	rawSizes  *[record.Fences]uint32
	rawFences *[record.Fences + 1]int64
}

// RawSize returns the bytes of fence i's records, so that a Join plans by
// records, as it does those in memory, whether or not they were
// compressed.
func (f *File) RawSize(i int) int64 {
	switch {
	case f.rawSizes != nil:
		return int64(f.rawSizes[i])
	case f.rawFences != nil:
		return f.rawFences[i+1] - f.rawFences[i]
	}
	return f.Fences[i+1] - f.Fences[i]
}

// FileBytes is the memory a File without blocks takes, which its run
// reserves for as long as it keeps the file.
const FileBytes = int64(unsafe.Sizeof(File{})) + 128

// MaxFileBytes is the most memory a File takes: one of blocks with a fence
// of 4 GiB or more.
const MaxFileBytes = FileBytes + int64(unsafe.Sizeof([record.Fences + 1]int64{}))

// Mem returns the memory f takes.
func (f *File) Mem() int64 {
	switch {
	case f.rawSizes != nil:
		return FileBytes + int64(unsafe.Sizeof([record.Fences]uint32{}))
	case f.rawFences != nil:
		return MaxFileBytes
	}
	return FileBytes
}

// FilesMem returns the memory files take.
func FilesMem(files []*File) int64 {
	var n int64
	for _, f := range files {
		n += f.Mem()
	}
	return n
}

// Segment returns the offsets of fences lo to hi-1 in the file.
func (f *File) Segment(lo, hi int) (off, end int64) { return f.Fences[lo], f.Fences[hi] }

// Align is the alignment of a direct write: its buffer's address, its
// size and its offset.
const Align = 4096

// AlignedBuffer returns a buffer of n bytes, rounded down to a multiple of
// Align, whose address is aligned for direct I/O.
func AlignedBuffer(n int) []byte {
	n &^= Align - 1
	raw := make([]byte, n+Align)
	skip := int(-uintptr(unsafe.Pointer(&raw[0])) & (Align - 1))
	return raw[skip : skip+n : skip+n]
}

// Writer writes one run file: records in order, through one aligned
// buffer, and the fence table.
type Writer struct {
	fsys   humus.FS
	f      *os.File
	file   *File
	buf    []byte
	off    int64 // of the next byte of the file
	next   int   // the first fence whose start isn't known yet
	direct bool
	err    error
	writes int64
	left   int                      // bytes of the record begun last not yet written
	enc    *Encoder                 // nil for a file without blocks
	large  bool                     // the record begun last is a block of its own
	raw    [record.Fences + 1]int64 // the bytes of records before each fence
}

// Create creates a run file at path, creating its directory, to be
// written through buf, which AlignedBuffer returned, in the Format of enc,
// or without blocks if enc is nil. The Writer uses buf and enc until
// Finish or Abort. The file is opened for direct I/O unless the filesystem
// refuses it.
func Create(fsys humus.FS, path string, buf []byte, enc *Encoder) (*Writer, error) {
	if len(buf) == 0 || len(buf)%Align != 0 || uintptr(unsafe.Pointer(&buf[0]))%Align != 0 {
		panic("spill: Create with a buffer AlignedBuffer didn't return")
	}
	if err := fsys.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create run file: %w", err)
	}
	flags := os.O_WRONLY | os.O_CREATE | os.O_EXCL
	direct := oDirect != 0
	f, err := fsys.OpenFile(path, flags|oDirect, 0o600)
	if direct && errors.Is(err, errInvalid) {
		// The filesystem doesn't do direct I/O (tmpfs before Linux 6.6,
		// some FUSE filesystems): write through the page cache.
		direct = false
		f, err = fsys.OpenFile(path, flags, 0o600)
	}
	if err != nil {
		return nil, fmt.Errorf("create run file: %w", err)
	}
	file := &File{Path: path}
	if enc != nil {
		file.Format = enc.format
		enc.raw = enc.raw[:0]
	}
	return &Writer{fsys: fsys, f: f, file: file, buf: buf[:0], direct: direct, enc: enc}, nil
}

// Direct reports whether the file is written with direct I/O.
func (w *Writer) Direct() bool { return w.direct }

// Writes returns the number of write calls so far.
func (w *Writer) Writes() int64 { return w.writes }

// Begin starts a record of hash and size bytes, which Write then adds.
// Records must come in order.
func (w *Writer) Begin(hash uint64, size int) {
	if w.left != 0 {
		panic("spill: Begin before the last record was written whole")
	}
	f := record.Fence(hash)
	if w.enc != nil {
		// A fence starts a block, and so does a record that doesn't fit the
		// block being filled.
		if block := w.enc.format.Block; w.next <= f || size > block || len(w.enc.raw)+size > block {
			w.endBlock()
		}
	}
	for ; w.next <= f; w.next++ {
		w.file.Fences[w.next] = w.off
		w.raw[w.next] = w.file.Raw
	}
	if w.enc != nil {
		w.large = size > w.enc.format.Block
		if w.large {
			var h [blockHeader]byte
			putLargeHeader(h[:], size)
			w.put(h[:])
		}
	}
	w.left = size
	w.file.Records++
	w.file.Raw += int64(size)
}

// Write adds bytes of the record begun last.
func (w *Writer) Write(p []byte) {
	if w.left -= len(p); w.left < 0 {
		panic("spill: Write past the size of the record begun last")
	}
	if w.enc != nil && !w.large {
		w.enc.raw = append(w.enc.raw, p...)
		return
	}
	w.put(p)
}

// endBlock writes the block being filled, compressed if that saves enough.
func (w *Writer) endBlock() {
	raw := w.enc.raw
	if len(raw) == 0 {
		return
	}
	kind, body := blockRaw, raw
	if c := w.enc.compress(raw); len(c) <= len(raw)-len(raw)/minSaving {
		kind, body = blockCompressed, c
	}
	var h [blockHeader]byte
	putBlockHeader(h[:], kind, len(raw), len(body))
	w.put(h[:])
	w.put(body)
	w.enc.raw = raw[:0]
}

// put adds bytes to the file. Every write to the file but the last is a
// full buffer, so that writes stay aligned.
func (w *Writer) put(p []byte) {
	w.off += int64(len(p))
	for len(p) > 0 && w.err == nil {
		n := copy(w.buf[len(w.buf):cap(w.buf)], p)
		w.buf, p = w.buf[:len(w.buf)+n], p[n:]
		if len(w.buf) == cap(w.buf) {
			w.flush()
		}
	}
}

// Add adds a whole record of hash.
func (w *Writer) Add(hash uint64, rec []byte) {
	w.Begin(hash, len(rec))
	w.Write(rec)
}

// Err returns the error of a write that failed.
func (w *Writer) Err() error { return w.err }

func (w *Writer) flush() {
	w.writes++
	_, err := w.f.Write(w.buf)
	if err != nil && w.direct && errors.Is(err, errInvalid) {
		// The filesystem took the flag at open but refuses the write.
		if err = clearDirect(w.f); err == nil {
			w.direct = false
			_, err = w.f.Write(w.buf)
		}
	}
	w.err = err
	w.buf = w.buf[:0]
}

// Finish writes what is left, closes the file and returns it. If a write
// failed, it removes the file and returns the error.
func (w *Writer) Finish() (*File, error) {
	if w.left != 0 {
		panic("spill: Finish before the last record was written whole")
	}
	if w.enc != nil {
		w.endBlock()
		if w.err == nil && w.off >= maxBlockedFile {
			w.err = fmt.Errorf("a file of blocks of %d bytes, more than a cursor's positions hold", w.off)
		}
	}
	if w.err == nil && len(w.buf) > 0 {
		n := len(w.buf)
		padded := false
		if w.direct {
			// A direct write is whole blocks: pad it, then cut the file.
			pad := (n + Align - 1) &^ (Align - 1)
			clear(w.buf[n:pad])
			w.buf, padded = w.buf[:pad], pad != n
		}
		w.flush()
		if w.err == nil && padded {
			w.err = w.f.Truncate(w.off)
		}
	}
	err := w.err
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = w.fsys.Remove(w.file.Path)
		return nil, fmt.Errorf("write run file: %w", err)
	}
	for ; w.next <= record.Fences; w.next++ {
		w.file.Fences[w.next] = w.off
		w.raw[w.next] = w.file.Raw
	}
	if w.enc != nil {
		w.file.rawSizes = new([record.Fences]uint32)
		for i := range w.file.rawSizes {
			n := w.raw[i+1] - w.raw[i]
			if n > math.MaxUint32 {
				wide := w.raw
				w.file.rawSizes, w.file.rawFences = nil, &wide
				break
			}
			w.file.rawSizes[i] = uint32(n)
		}
	}
	w.file.Bytes = w.off
	return w.file, nil
}

// Abort closes and removes the file.
func (w *Writer) Abort() {
	_ = w.f.Close()
	_ = w.fsys.Remove(w.file.Path)
}

// Copy writes the records of m to w, in order. A record whose value its
// source doesn't hold is copied through the source.
func Copy(ctx context.Context, w *Writer, m *merge.Merger) error {
	for n := 1; ; n++ {
		src := m.Top()
		if src == nil {
			return w.Err()
		}
		if n&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return context.Cause(ctx)
			}
		}
		head := src.Head()
		h, _ := record.ParseHeader(head)
		if len(head) == h.Size() {
			w.Add(h.Hash, head)
		} else {
			w.Begin(h.Hash, h.Size())
			w.Write(head)
			c, ok := src.(*Cursor)
			if !ok {
				return errors.New("spill: a source without its value that isn't a run file")
			}
			if err := c.CopyValue(w); err != nil {
				return err
			}
		}
		if err := w.Err(); err != nil {
			return err
		}
		if err := m.Pop(); err != nil {
			return err
		}
	}
}

// Open opens a run file for a join's or a merge's preads, with the
// kernel's readahead off: they read exactly their buffers, and take no
// page cache to read ahead.
func Open(fsys humus.FS, f *File) (*os.File, error) {
	r, err := fsys.Open(f.Path)
	if err != nil {
		return nil, fmt.Errorf("open run file: %w", err)
	}
	noReadahead(r)
	return r, nil
}
