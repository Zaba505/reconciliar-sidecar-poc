package harness

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Inode flags of linux/fs.h, which golang.org/x/sys/unix doesn't define.
// New files inherit them from their directory on btrfs.
const (
	flagCompress   = 0x00000004 // FS_COMPR_FL, chattr +c
	flagNoCompress = 0x00000400 // FS_NOCOMP_FL, chattr +m
	flagNoCOW      = 0x00800000 // FS_NOCOW_FL, chattr +C
)

// Filesystem describes the filesystem a directory is on. A filesystem that
// compresses the spill files written there hides what spilling costs, so
// the measurements record it.
type Filesystem struct {
	Dir     string // the directory, symbolic links resolved
	Mount   string // the mount point it is under
	Type    string // such as btrfs or ext4
	Source  string // what is mounted, such as a device
	Options string // the mount's options, then its superblock's

	// NoCompress is btrfs's no-compress attribute on the directory
	// (chattr +m), which new files in it inherit.
	NoCompress bool
	// Compresses reports whether the filesystem may compress the files
	// written in the directory. Only btrfs's compression is recognized.
	Compresses bool
}

// DescribeFilesystem describes the filesystem dir is on, from the mount
// whose mount point is the longest prefix of dir in /proc/self/mountinfo
// and, on btrfs, the directory's inode flags.
func DescribeFilesystem(dir string) (Filesystem, error) {
	dir, err := filepath.Abs(dir)
	if err == nil {
		dir, err = filepath.EvalSymlinks(dir)
	}
	if err != nil {
		return Filesystem{}, err
	}
	f, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return Filesystem{}, err
	}
	defer f.Close()
	fs, err := mountOf(f, dir)
	if err != nil {
		return Filesystem{}, err
	}
	if fs.Type != "btrfs" {
		return fs, nil
	}
	flags, err := inodeFlags(dir)
	if err != nil {
		return Filesystem{}, err
	}
	fs.NoCompress = flags&flagNoCompress != 0
	fs.Compresses = btrfsCompresses(fs.Options, flags)
	return fs, nil
}

// mountOf returns the mount of mountinfo, in the format of
// /proc/self/mountinfo, whose mount point is the longest prefix of dir, an
// absolute path. Of mounts on the same point, the last is the one on top.
func mountOf(mountinfo io.Reader, dir string) (Filesystem, error) {
	fs := Filesystem{Dir: dir}
	sc := bufio.NewScanner(mountinfo)
	for sc.Scan() {
		// ID, parent ID, major:minor, root, mount point, mount options,
		// optional fields, "-", type, source, superblock options.
		pre, post, ok := strings.Cut(sc.Text(), " - ")
		fields, super := strings.Fields(pre), strings.Fields(post)
		if !ok || len(fields) < 6 || len(super) < 3 {
			return Filesystem{}, fmt.Errorf("mountinfo: malformed line %q", sc.Text())
		}
		point := unescapeMount(fields[4])
		if !under(dir, point) || len(point) < len(fs.Mount) {
			continue
		}
		fs.Mount, fs.Type, fs.Source = point, super[0], unescapeMount(super[1])
		fs.Options = mergeOptions(fields[5], super[2])
	}
	if err := sc.Err(); err != nil {
		return Filesystem{}, fmt.Errorf("mountinfo: %w", err)
	}
	if fs.Mount == "" {
		return Filesystem{}, fmt.Errorf("mountinfo: no mount holds %s", dir)
	}
	return fs, nil
}

// under reports whether path is dir or lies below it.
func under(path, dir string) bool {
	return path == dir || dir == "/" || strings.HasPrefix(path, dir+"/")
}

// unescapeMount undoes mountinfo's octal escapes, such as \040 for a space.
func unescapeMount(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+4 <= len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// mergeOptions joins a mount's options and its superblock's, leaving out
// the superblock's that repeat one of the mount's, as findmnt does.
func mergeOptions(mount, super string) string {
	opts := strings.Split(mount, ",")
	for o := range strings.SplitSeq(super, ",") {
		if !slices.Contains(opts, o) {
			opts = append(opts, o)
		}
	}
	return strings.Join(opts, ",")
}

// inodeFlags returns dir's inode flags, as lsattr shows them.
func inodeFlags(dir string) (uint32, error) {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return 0, &os.PathError{Op: "open", Path: dir, Err: err}
	}
	defer unix.Close(fd)
	flags, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if err != nil {
		return 0, &os.PathError{Op: "get inode flags", Path: dir, Err: err}
	}
	return flags, nil
}

// btrfsCompresses reports whether btrfs, mounted with options, may compress
// a new file in a directory with flags, which the file inherits. It
// follows the kernel's inode_need_compress: a no-COW file is never
// compressed; compress-force compresses any other; otherwise the
// no-compress attribute wins over compress and the compress attribute.
func btrfsCompresses(options string, flags uint32) bool {
	var compress, force bool
	for o := range strings.SplitSeq(options, ",") {
		name, value, _ := strings.Cut(o, "=")
		switch name {
		case "compress":
			compress = value != "no"
		case "compress-force":
			force = value != "no"
		}
	}
	switch {
	case flags&flagNoCOW != 0:
		return false
	case force:
		return true
	case flags&flagNoCompress != 0:
		return false
	}
	return compress || flags&flagCompress != 0
}
