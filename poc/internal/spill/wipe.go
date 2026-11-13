// Package spill holds the sidecar's spill files on its data directory.
package spill

import (
	"fmt"
	"path/filepath"

	"github.com/z5labs/humus"
)

// Wipe removes everything inside dir but not dir itself, which is usually
// a mount point that the sidecar can't recreate.
func Wipe(fsys humus.FS, dir string) error {
	entries, err := fsys.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("wipe data dir: %w", err)
	}
	for _, e := range entries {
		if err := fsys.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return fmt.Errorf("wipe data dir: %w", err)
		}
	}
	return nil
}
