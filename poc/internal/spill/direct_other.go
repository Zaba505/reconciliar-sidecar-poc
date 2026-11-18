//go:build !linux

package spill

import (
	"errors"
	"os"
)

// Direct I/O is Linux's: elsewhere run files go through the page cache.
const oDirect = 0

var errInvalid = errors.New("spill: invalid argument")

func clearDirect(*os.File) error { return nil }

func noReadahead(*os.File) {}
