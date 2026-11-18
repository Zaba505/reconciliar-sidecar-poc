package spill

import (
	"os"

	"golang.org/x/sys/unix"
)

const oDirect = unix.O_DIRECT

var errInvalid error = unix.EINVAL

// clearDirect turns direct I/O off for f.
func clearDirect(f *os.File) error {
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	_, err = unix.FcntlInt(f.Fd(), unix.F_SETFL, flags&^unix.O_DIRECT)
	return err
}

func noReadahead(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM)
}
