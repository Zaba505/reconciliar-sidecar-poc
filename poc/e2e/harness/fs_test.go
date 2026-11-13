package harness

import (
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
)

func TestMountOf(t *testing.T) {
	const mountinfo = `46 1 0:38 /root / rw,relatime shared:1 - btrfs /dev/nvme0n1p3 rw,seclabel,compress=zstd:1,ssd,subvolid=257,subvol=/root
60 46 0:39 /home /home rw,relatime shared:30 - btrfs /dev/nvme0n1p3 rw,seclabel,compress=zstd:1,ssd,subvolid=256,subvol=/home
70 46 0:40 / /tmp rw,nosuid,nodev shared:40 - tmpfs tmpfs rw,seclabel,size=16G
80 46 259:5 / /mnt/My\040Disk rw,relatime - ext4 /dev/disk/by-label/My\040Disk rw
90 60 0:41 / /home/x rw shared:60 - tmpfs tmpfs rw
91 60 259:6 / /home/x rw shared:61 - xfs /dev/sdb rw,noquota
`
	for _, tc := range []struct {
		dir, mount, typ, source, options string
	}{
		{"/var/tmp", "/", "btrfs", "/dev/nvme0n1p3", "rw,relatime,seclabel,compress=zstd:1,ssd,subvolid=257,subvol=/root"},
		{"/", "/", "btrfs", "/dev/nvme0n1p3", "rw,relatime,seclabel,compress=zstd:1,ssd,subvolid=257,subvol=/root"},
		{"/home/u/data", "/home", "btrfs", "/dev/nvme0n1p3", "rw,relatime,seclabel,compress=zstd:1,ssd,subvolid=256,subvol=/home"},
		{"/homework", "/", "btrfs", "/dev/nvme0n1p3", "rw,relatime,seclabel,compress=zstd:1,ssd,subvolid=257,subvol=/root"},
		{"/tmp", "/tmp", "tmpfs", "tmpfs", "rw,nosuid,nodev,seclabel,size=16G"},
		{"/mnt/My Disk/runs", "/mnt/My Disk", "ext4", "/dev/disk/by-label/My Disk", "rw,relatime"},
		{"/home/x/y", "/home/x", "xfs", "/dev/sdb", "rw,noquota"}, // mounted over the tmpfs
	} {
		got, err := mountOf(strings.NewReader(mountinfo), tc.dir)
		want := Filesystem{Dir: tc.dir, Mount: tc.mount, Type: tc.typ, Source: tc.source, Options: tc.options}
		if err != nil || got != want {
			t.Errorf("mountOf(%q) = %+v, %v; want %+v", tc.dir, got, err, want)
		}
	}
	for _, bad := range []string{"46 1 0:38 /root / rw\n", "46 1 0:38 /root / rw - btrfs\n", ""} {
		if got, err := mountOf(strings.NewReader(bad), "/var/tmp"); err == nil {
			t.Errorf("mountOf(%q) = %+v, want an error", bad, got)
		}
	}
}

func TestBtrfsCompresses(t *testing.T) {
	for _, tc := range []struct {
		options string
		flags   uint32
		want    bool
	}{
		{"rw,relatime,space_cache=v2", 0, false},
		{"rw,compress=zstd:1,ssd", 0, true},
		{"rw,compress,ssd", 0, true},
		{"rw,compress=no", 0, false},
		{"rw,compress=zstd:1", flagNoCompress, false},
		{"rw,compress-force=zstd:3", 0, true},
		{"rw,compress-force=zstd:3", flagNoCompress, true}, // force wins over +m
		{"rw,compress-force=zstd:3", flagNoCOW, false},     // but not over +C
		{"rw,compress=zstd:1", flagNoCOW, false},
		{"rw", flagCompress, true},
		{"rw", flagCompress | flagNoCompress, false},
	} {
		if got := btrfsCompresses(tc.options, tc.flags); got != tc.want {
			t.Errorf("btrfsCompresses(%q, %#x) = %t, want %t", tc.options, tc.flags, got, tc.want)
		}
	}
}

// TestDescribeFilesystem describes directories of the machine it runs on.
// The checks of btrfs need /var/tmp on btrfs mounted with compress, and
// /var/tmp/recon-e2e-nocomp made with chattr +m, and are skipped without
// them.
func TestDescribeFilesystem(t *testing.T) {
	tmp := t.TempDir()
	got, err := DescribeFilesystem(tmp)
	if err != nil || got.Type == "" || got.Mount == "" || !under(got.Dir, got.Mount) {
		t.Fatalf("DescribeFilesystem(%q) = %+v, %v; want a type and a mount point above it", tmp, got, err)
	}
	if _, err := DescribeFilesystem(tmp + "/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("DescribeFilesystem of a missing directory: %v, want ErrNotExist", err)
	}

	varTmp, err := DescribeFilesystem("/var/tmp")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("/var/tmp: %+v", varTmp)
	if varTmp.Type != "btrfs" || !strings.Contains(varTmp.Options, "compress") {
		t.Skip("/var/tmp is not btrfs mounted with compression")
	}
	if !varTmp.Compresses || varTmp.NoCompress {
		t.Errorf("/var/tmp: %+v, want Compresses and not NoCompress", varTmp)
	}

	const nocomp = "/var/tmp/recon-e2e-nocomp"
	if _, err := os.Stat(nocomp); err != nil {
		t.Skipf("no %s: %v", nocomp, err)
	}
	// The harness makes the data directories below the data root, and they
	// must inherit the attribute.
	sub, err := os.MkdirTemp(nocomp, "harness-test-")
	if err != nil {
		t.Skip(err)
	}
	defer func() { _ = os.Remove(sub) }()
	for _, dir := range []string{nocomp, sub} {
		got, err := DescribeFilesystem(dir)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %+v", dir, got)
		// Only compress-force overrides the attribute.
		force := strings.Contains(got.Options, "compress-force")
		if !got.NoCompress || got.Compresses != force {
			t.Errorf("%s: %+v, want NoCompress, and Compresses only with compress-force", dir, got)
		}
	}
}
