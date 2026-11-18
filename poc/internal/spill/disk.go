package spill

import (
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/z5labs/humus"
)

// ErrDiskExhausted is returned by Reserve when a run file would take the
// spill files past the disk limit.
var ErrDiskExhausted = errors.New("disk exhausted")

// Disk keeps the total size of the run files under a limit. A file's bytes
// are reserved before it is written and released once it is removed. It
// is safe for concurrent use.
type Disk struct {
	fsys  humus.FS
	limit int64
	used  atomic.Int64
	peak  atomic.Int64
}

// NewDisk returns a Disk on fsys that holds at most limit bytes of run
// files.
func NewDisk(fsys humus.FS, limit int64) *Disk {
	return &Disk{fsys: fsys, limit: limit}
}

// FS returns the filesystem the files are on.
func (d *Disk) FS() humus.FS { return d.fsys }

// Limit returns the most bytes of run files the Disk holds.
func (d *Disk) Limit() int64 { return d.limit }

// Used returns the bytes of run files reserved.
func (d *Disk) Used() int64 { return d.used.Load() }

// Peak returns the most bytes of run files that were ever reserved at once.
func (d *Disk) Peak() int64 { return d.peak.Load() }

// Reserve reserves n bytes for a file about to be written. It fails with an
// error wrapping ErrDiskExhausted if they would exceed the limit.
func (d *Disk) Reserve(n int64) error {
	for {
		used := d.used.Load()
		if used+n > d.limit {
			return fmt.Errorf("%w: spilling %d bytes with %d of %d in use", ErrDiskExhausted, n, used, d.limit)
		}
		if d.used.CompareAndSwap(used, used+n) {
			d.raisePeak(used + n)
			return nil
		}
	}
}

func (d *Disk) raisePeak(used int64) {
	for {
		p := d.peak.Load()
		if used <= p || d.peak.CompareAndSwap(p, used) {
			return
		}
	}
}

// Release gives back n bytes of files that were removed, or that were
// reserved and not written.
func (d *Disk) Release(n int64) {
	if d.used.Add(-n) < 0 {
		panic(fmt.Sprintf("spill: Release(%d) of more than was reserved", n))
	}
}
