package spill

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/z5labs/humus"
)

// ErrDiskExhausted is returned by Write when a file would take the spill
// files past the disk limit.
var ErrDiskExhausted = errors.New("disk exhausted")

// File is one spill file in a run's manifest. It holds records encoded as
// buffer chunks hold them, back to back, uncompressed.
type File struct {
	Path    string
	Records int64
	Bytes   int64
}

// Disk writes and reads spill files, keeping their total size under a
// limit. It is safe for concurrent use.
type Disk struct {
	fsys  humus.FS
	limit int64
	used  atomic.Int64
	peak  atomic.Int64
}

// NewDisk returns a Disk on fsys that holds at most limit bytes of spill
// files.
func NewDisk(fsys humus.FS, limit int64) *Disk {
	return &Disk{fsys: fsys, limit: limit}
}

// Used returns the bytes of spill files on disk.
func (d *Disk) Used() int64 { return d.used.Load() }

// Peak returns the most bytes of spill files that were ever on disk at
// once.
func (d *Disk) Peak() int64 { return d.peak.Load() }

// Write writes chunks, which hold records records, to a new file at path,
// creating its directory. The file is closed when Write returns, and it
// counts against the limit until Release. Writing fails with an error
// wrapping ErrDiskExhausted if it would exceed the limit, and if path
// exists. A failed Write leaves no file behind.
func (d *Disk) Write(path string, records int64, chunks [][]byte) (File, error) {
	var n int64
	for _, c := range chunks {
		n += int64(len(c))
	}
	var used int64
	for {
		used = d.used.Load()
		if used+n > d.limit {
			return File{}, fmt.Errorf("%w: spilling %d bytes with %d of %d in use", ErrDiskExhausted, n, used, d.limit)
		}
		if d.used.CompareAndSwap(used, used+n) {
			break
		}
	}
	if err := d.write(path, chunks); err != nil {
		d.used.Add(-n)
		return File{}, fmt.Errorf("write spill file: %w", err)
	}
	d.raisePeak(used + n)
	return File{Path: path, Records: records, Bytes: n}, nil
}

func (d *Disk) raisePeak(used int64) {
	for {
		p := d.peak.Load()
		if used <= p || d.peak.CompareAndSwap(p, used) {
			return
		}
	}
}

func (d *Disk) write(path string, chunks [][]byte) error {
	if err := d.fsys.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// Records are opaque and runs don't survive a restart, so the file
	// isn't synced.
	f, err := d.fsys.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		if _, err = f.Write(c); err != nil {
			break
		}
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = d.fsys.Remove(path)
	}
	return err
}

// Read reads the whole of f.
func (d *Disk) Read(f File) ([]byte, error) {
	r, err := d.fsys.Open(f.Path)
	if err != nil {
		return nil, fmt.Errorf("read spill file: %w", err)
	}
	defer r.Close()
	b := make([]byte, f.Bytes)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, fmt.Errorf("read spill file %s: %w", f.Path, err)
	}
	return b, nil
}

// Open opens f for reading.
func (d *Disk) Open(f File) (io.ReadCloser, error) {
	r, err := d.fsys.Open(f.Path)
	if err != nil {
		return nil, fmt.Errorf("read spill file: %w", err)
	}
	return r, nil
}

// Release gives back n bytes of files that were removed.
func (d *Disk) Release(n int64) {
	if d.used.Add(-n) < 0 {
		panic(fmt.Sprintf("spill: Release(%d) of more than was written", n))
	}
}
