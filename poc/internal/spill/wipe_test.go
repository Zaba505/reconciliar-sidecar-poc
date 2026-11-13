package spill

import (
	"os"
	"testing"

	"github.com/z5labs/humus/humustest"
)

func TestWipe_removes_the_contents_but_keeps_the_directory(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{
		"data/runs/0190/left/3/0.rec": []byte("x"),
		"data/runs/0190/right/1.rec":  []byte("y"),
		"data/stray":                  []byte("z"),
		"other/keep":                  []byte("k"),
	}))
	if err := Wipe(h.FS(), h.Path("data")); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(h.Path("data"))
	if err != nil {
		t.Fatalf("data dir is gone: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("data dir holds %d entries, want none", len(entries))
	}
	if _, err := os.Stat(h.Path("other/keep")); err != nil {
		t.Errorf("a file outside the data dir was removed: %v", err)
	}
}

func TestWipe_an_empty_directory(t *testing.T) {
	h := humustest.NewHost(t, humustest.WithFiles(map[string][]byte{"data/.keep": nil}))
	if err := os.Remove(h.Path("data/.keep")); err != nil {
		t.Fatal(err)
	}
	if err := Wipe(h.FS(), h.Path("data")); err != nil {
		t.Fatal(err)
	}
}

func TestWipe_a_missing_directory_fails(t *testing.T) {
	h := humustest.NewHost(t)
	if err := Wipe(h.FS(), h.Path("missing")); err == nil {
		t.Error("Wipe of a missing directory succeeded")
	}
}
