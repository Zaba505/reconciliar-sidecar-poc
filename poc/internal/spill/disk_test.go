package spill

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/z5labs/humus/humustest"
)

func newDisk(t *testing.T, limit int64) (*Disk, string) {
	t.Helper()
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	return NewDisk(h.FS(), limit), h.Path("data")
}

func TestDisk_writes_and_reads_a_file(t *testing.T) {
	d, dir := newDisk(t, 1<<20)
	path := filepath.Join(dir, "runs", "r", "left", "3", "0.rec")
	chunks := [][]byte{[]byte("abc"), nil, []byte("defg")}
	f, err := d.Write(path, 2, chunks)
	if err != nil {
		t.Fatal(err)
	}
	if f != (File{Path: path, Records: 2, Bytes: 7}) {
		t.Errorf("Write = %+v", f)
	}
	if d.Used() != 7 {
		t.Errorf("Used = %d, want 7", d.Used())
	}
	got, err := d.Read(f)
	if err != nil || !bytes.Equal(got, []byte("abcdefg")) {
		t.Errorf("Read = %q, %v", got, err)
	}
	if _, err := d.Write(path, 1, chunks); !errors.Is(err, os.ErrExist) {
		t.Errorf("a second Write to %s: %v, want ErrExist", path, err)
	}
	if got, err := d.Read(f); err != nil || !bytes.Equal(got, []byte("abcdefg")) {
		t.Errorf("Read after the refused Write = %q, %v; want the first file intact", got, err)
	}
	if d.Used() != 7 {
		t.Errorf("Used = %d after the failed Write, want 7", d.Used())
	}
	d.Release(7)
	if d.Used() != 0 || d.Peak() != 7 {
		t.Errorf("Used = %d, Peak = %d after Release, want 0 and 7", d.Used(), d.Peak())
	}
}

func TestDisk_refuses_a_file_past_its_limit(t *testing.T) {
	d, dir := newDisk(t, 10)
	if _, err := d.Write(filepath.Join(dir, "a.rec"), 1, [][]byte{make([]byte, 6)}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "b.rec")
	if _, err := d.Write(path, 1, [][]byte{make([]byte, 5)}); !errors.Is(err, ErrDiskExhausted) {
		t.Fatalf("Write past the limit: %v, want ErrDiskExhausted", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused file: %v, want none", err)
	}
	if d.Used() != 6 {
		t.Errorf("Used = %d, want 6", d.Used())
	}
	if _, err := d.Write(path, 1, [][]byte{make([]byte, 4)}); err != nil {
		t.Errorf("a Write that fits: %v", err)
	}
	if d.Peak() != 10 {
		t.Errorf("Peak = %d, want 10", d.Peak())
	}
}

func TestDisk_Read_of_a_short_file_fails(t *testing.T) {
	d, dir := newDisk(t, 100)
	f, err := d.Write(filepath.Join(dir, "a.rec"), 1, [][]byte{[]byte("abc")})
	if err != nil {
		t.Fatal(err)
	}
	f.Bytes = 4
	if _, err := d.Read(f); err == nil {
		t.Error("Read of a file shorter than its manifest entry succeeded")
	}
}
