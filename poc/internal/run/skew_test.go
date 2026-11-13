package run

import (
	"context"
	"errors"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"google.golang.org/protobuf/proto"

	reconv1 "poc/gen/recon/v1"
	"poc/internal/join"
	"poc/internal/oracle"
)

var scratchPath = regexp.MustCompile(`^join-[0-9]+/(left|right)/[0-9]+/[0-9]+\.rec$`)

// scratchFiles returns the files under a run's join-<n> directories,
// relative to the run's, and fails the test on a file that doesn't follow
// the layout.
func scratchFiles(t *testing.T, dir string) []string {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(dir, "join-*"))
	if err != nil {
		t.Fatal(err)
	}
	var files []string
	for _, d := range dirs {
		err := filepath.WalkDir(d, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			rel, _ := filepath.Rel(dir, path)
			if !scratchPath.MatchString(rel) {
				t.Errorf("sub-partition file %s isn't at join-<n>/<side>/<bucket>/<seq>.rec", rel)
			}
			files = append(files, rel)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(files) == 0 && len(dirs) > 0 {
		t.Errorf("empty sub-partition directories %v", dirs)
	}
	return files
}

func TestRun_Join_subpartitions_under_the_run_directory(t *testing.T) {
	e := newEnv(t, 256<<10)
	id, r := e.create(t, 1)
	msgs, records := messages(randomRecords(rand.New(rand.NewPCG(8, 8)), 6000, 2000, 100), 100)
	if err := ingestStreams(r, 2, msgs); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	disk := e.disk.Used()
	var got []*reconv1.JoinResult
	var during int
	err := r.Join(t.Context(), join.NewFilter(nil), func(resp *reconv1.JoinResponse) error {
		during = max(during, len(scratchFiles(t, r.dir)))
		got = append(got, proto.Clone(resp).(*reconv1.JoinResponse).GetResults()...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if d := oracle.Diff(oracle.Join(records), got); d != "" {
		t.Error(d)
	}
	if during == 0 {
		t.Error("no sub-partition files while the Join ran; want the 400 KiB bucket sub-partitioned")
	}
	if files := scratchFiles(t, r.dir); len(files) != 0 || e.disk.Used() != disk {
		t.Errorf("after the Join: sub-partition files %v, %d bytes on disk, %d before", files, e.disk.Used(), disk)
	}
	if err := e.m.Delete(id); err != nil {
		t.Fatal(err)
	}
	if e.pool.Used() != 0 || e.disk.Used() != 0 {
		t.Errorf("after Delete: %d bytes reserved, %d on disk", e.pool.Used(), e.disk.Used())
	}
}

func TestRun_Delete_during_a_sub_partitioned_Join(t *testing.T) {
	e := newEnv(t, 256<<10)
	id, r := e.create(t, 1)
	msgs, _ := messages(randomRecords(rand.New(rand.NewPCG(9, 9)), 6000, 2000, 100), 100)
	if err := ingestStreams(r, 1, msgs); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Seal(); err != nil {
		t.Fatal(err)
	}
	// The other worker joins the empty-key unit, whose batches may come
	// first.
	var deleted bool
	err := r.Join(context.Background(), join.NewFilter(nil), func(*reconv1.JoinResponse) error {
		if deleted || len(scratchFiles(t, r.dir)) == 0 {
			return nil
		}
		deleted = true
		return e.m.Delete(id)
	})
	if !deleted {
		t.Fatalf("Join: %v, with no batch sent while sub-partition files existed", err)
	}
	if !errors.Is(err, ErrAborted) {
		t.Fatalf("Join: %v, want ErrAborted", err)
	}
	if _, err := os.Stat(r.dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("run directory after the Join: %v, want it gone", err)
	}
	if e.pool.Used() != 0 || e.disk.Used() != 0 {
		t.Errorf("after the Join: %d bytes reserved, %d on disk", e.pool.Used(), e.disk.Used())
	}
}
