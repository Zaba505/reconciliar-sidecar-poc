// Command datastats prints how large and how compressible values are in the
// sidecar's spill format: percentiles of the value sizes, and the s2 and
// zstd level 1 ratios of the spill-format records, compressed in 64 KiB
// blocks. It calibrates the e2e generator's profiles against real data.
//
// It samples one of:
//
//	datastats                                     every profile
//	datastats -profile Payment -encoding avro     a profile, in one encoding or all
//	datastats -schema payment.json                a schema file
//	datastats -values real.bin -framing varint    a file of real values
//
// A schema file is the JSON form of gen.Schema. To start one from a
// profile, dump it:
//
//	datastats -profile Payment -dump > payment.json
//
// A file of real values holds each value after its length as a uvarint
// (-framing varint), or one value per line (-framing lines). The i-th
// value gets key i, in the format -keys names.
//
// Generated values are one side's records (-side), in key order, with
// -mix deciding how many each key has. Every sample stops after about
// -bytes of spill-format records. The defaults (seed 20, RF keys and
// 96 MiB samples) are the reference prototype's, so the table compares
// with the one it measured.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"iter"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"poc/e2e/gen"
	"poc/e2e/gen/stats"
	"poc/e2e/harness"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "datastats:", err)
		os.Exit(1)
	}
}

// maxValue is the largest value a file may hold, so that a file with the
// wrong framing fails instead of claiming a huge value.
const maxValue = 1 << 30

// keyRange is how many keys generated values may use: as many as decimal
// keys allow, so that a sample never runs out.
const keyRange = 1_000_000_000_000

// run runs the command with args, writing the table to out. Like the
// flag package's command line, it exits on a flag it can't parse.
func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("datastats", flag.ExitOnError)
	var (
		profile  = fs.String("profile", "", "a built-in profile `name`: Payment, BankLine, CardSettlement, Envelope or Tiny (default every one)")
		schema   = fs.String("schema", "", "a schema `file`, the JSON form of gen.Schema")
		valFile  = fs.String("values", "", "a `file` of real values")
		framing  = fs.String("framing", "", "how -values are framed: varint (a uvarint length before each) or lines (one per line)")
		encoding = fs.String("encoding", "all", "the `encoding` of generated values: protobuf, avro, json or all")
		dump     = fs.Bool("dump", false, "print the schema of -profile or -schema as JSON, and exit")
		seed     = fs.Uint64("seed", 20, "the generator's seed")
		side     = harness.Left
		mix      = gen.Typical
		keys     = gen.RFKeys
		limit    = int64(96 << 20)
	)
	fs.Func("side", "the `side` to sample: left or right (default left)", func(s string) error {
		switch strings.ToLower(s) {
		case "left":
			side = harness.Left
		case "right":
			side = harness.Right
		default:
			return errors.New("want left or right")
		}
		return nil
	})
	fs.TextVar(&mix, "mix", mix, "how many records each side of a key has: stress or typical")
	fs.TextVar(&keys, "keys", keys, "the key format: decimal or rf")
	fs.Func("bytes", "the `size` of each sample in spill-format bytes, such as 64MiB (default 96MiB)", func(s string) error {
		n, err := parseBytes(s)
		if err == nil && n == 0 {
			err = errors.New("a sample needs at least 1 byte")
		}
		limit = n
		return err
	})
	fs.Parse(args)
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q", fs.Args())
	}
	if countSet(*profile, *schema, *valFile) > 1 {
		return errors.New("give one of -profile, -schema and -values")
	}
	c := gen.Config{Seed: *seed, Keys: keyRange, Mix: mix, KeyFormat: keys}

	if *valFile != "" {
		if *dump {
			return errors.New("-dump needs -profile or -schema")
		}
		return sampleFile(out, c, side, *valFile, *framing, limit)
	}
	if *framing != "" {
		return errors.New("-framing needs -values")
	}

	var schemas []*gen.Schema
	switch {
	case *schema != "":
		s, err := readSchema(*schema)
		if err != nil {
			return err
		}
		schemas = []*gen.Schema{s}
	case *profile != "":
		s := gen.Profile(*profile)
		if s == nil {
			return fmt.Errorf("unknown profile %q", *profile)
		}
		schemas = []*gen.Schema{s}
	default:
		if *dump {
			return errors.New("-dump needs -profile or -schema")
		}
		schemas = gen.Profiles()
	}
	if *dump {
		enc := json.NewEncoder(out)
		enc.SetEscapeHTML(false)
		enc.SetIndent("", "  ")
		return enc.Encode(schemas[0])
	}

	encs := []gen.Encoding{gen.Protobuf, gen.Avro, gen.JSON}
	if !strings.EqualFold(*encoding, "all") {
		e, err := gen.ParseEncoding(*encoding)
		if err != nil {
			return err
		}
		encs = []gen.Encoding{e}
	}
	t := table{w: out, label: "profile"}
	for _, s := range schemas {
		t.width = max(t.width, len(s.Name))
	}
	for _, s := range schemas {
		for _, e := range encs {
			c.Left = gen.Values{Schema: s, Encoding: e}
			c.Right = c.Left
			if err := c.Validate(); err != nil {
				return err
			}
			t.row(s.Name, e.String(), stats.Sample(generated(c, side), limit))
		}
	}
	return t.err
}

// countSet returns how many of ss are not empty.
func countSet(ss ...string) int {
	n := 0
	for _, s := range ss {
		if s != "" {
			n++
		}
	}
	return n
}

// generated yields c's records on side, in key order. It reuses the key
// and value buffers, which stats.Sample allows.
func generated(c gen.Config, side harness.Side) iter.Seq[harness.Record] {
	return func(yield func(harness.Record) bool) {
		g := c.ValueGen()
		var key, val []byte
		for k := range c.Keys {
			n, m := c.Counts(k)
			if side == harness.Right {
				n = m
			}
			for j := range n {
				key = c.AppendKey(key[:0], k)
				val = g.Append(val[:0], side, k, j)
				if !yield(harness.Record{Side: side, Key: key, Value: val}) {
					return
				}
			}
		}
	}
}

// readSchema reads a schema file and validates it.
func readSchema(path string) (*gen.Schema, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s, err := parseSchema(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// parseSchema parses the JSON form of a schema and validates it. Unknown
// fields are an error, so that a misspelt parameter isn't ignored.
func parseSchema(b []byte) (*gen.Schema, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var s gen.Schema
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	if dec.Decode(new(json.RawMessage)) != io.EOF {
		return nil, errors.New("data after the schema")
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// sampleFile prints the row of a file of real values.
func sampleFile(out io.Writer, c gen.Config, side harness.Side, path, framing string, limit int64) error {
	if framing != "varint" && framing != "lines" {
		return errors.New("-values needs -framing varint or lines")
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var readErr error
	records := func(yield func(harness.Record) bool) {
		var key []byte
		i := 0
		for v := range readValues(f, framing, &readErr) {
			key = c.AppendKey(key[:0], i)
			if !yield(harness.Record{Side: side, Key: key, Value: v}) {
				return
			}
			i++
		}
	}
	r := stats.Sample(records, limit)
	if readErr != nil {
		return fmt.Errorf("%s: %w", path, readErr)
	}
	name := filepath.Base(path)
	t := table{w: out, label: "file", width: len(name)}
	t.row(name, "-", r)
	return t.err
}

// readValues yields the values in r, framed as framing says, reusing one
// buffer. Lines drop a carriage return before the newline. A read that
// fails or finds a malformed value ends them, and sets *err.
func readValues(r io.Reader, framing string, err *error) iter.Seq[[]byte] {
	if framing == "lines" {
		return func(yield func([]byte) bool) {
			sc := bufio.NewScanner(r)
			sc.Buffer(make([]byte, 64<<10), maxValue)
			for sc.Scan() {
				if !yield(sc.Bytes()) {
					return
				}
			}
			*err = sc.Err()
		}
	}
	return func(yield func([]byte) bool) {
		br := bufio.NewReaderSize(r, 1<<20)
		var buf []byte
		for i := 0; ; i++ {
			n, e := binary.ReadUvarint(br)
			switch {
			case e == io.EOF:
				return
			case e != nil:
				*err = fmt.Errorf("the length of value %d: %w", i, e)
				return
			case n > maxValue:
				*err = fmt.Errorf("value %d claims %d bytes, more than %d: is the framing right?", i, n, maxValue)
				return
			}
			buf = slices.Grow(buf[:0], int(n))[:n]
			if _, e := io.ReadFull(br, buf); e != nil {
				if e == io.EOF {
					e = io.ErrUnexpectedEOF
				}
				*err = fmt.Errorf("value %d of %d bytes: %w", i, n, e)
				return
			}
			if !yield(buf) {
				return
			}
		}
	}
}

// table prints Reports as aligned rows, under a header.
type table struct {
	w      io.Writer
	label  string // what the first column holds
	width  int    // of the first column
	header bool   // printed already
	err    error  // the first write error
}

func (t *table) row(name, enc string, r stats.Report) {
	if !t.header {
		t.header = true
		t.printf("%-*s  %-8s  %8s  %8s  %7s  %7s  %7s  %8s  %6s  %6s\n",
			max(t.width, len(t.label)), t.label, "encoding", "records", "mean", "p50", "p90", "p99", "max", "s2", "zstd1")
	}
	t.printf("%-*s  %-8s  %8d  %8.1f  %7d  %7d  %7d  %8d  %5.2fx  %5.2fx\n",
		max(t.width, len(t.label)), name, enc, r.Records, r.Value.Mean,
		r.Value.P50, r.Value.P90, r.Value.P99, r.Value.Max, r.S2Ratio, r.ZstdRatio)
}

func (t *table) printf(format string, a ...any) {
	if t.err == nil {
		_, t.err = fmt.Fprintf(t.w, format, a...)
	}
}

// byteUnits are the suffixes parseBytes accepts, longest first so that
// "KiB" is tried before "B".
var byteUnits = []struct {
	suffix string
	scale  int64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
	{"B", 1},
}

// parseBytes parses a byte count: a non-negative decimal integer with an
// optional IEC suffix, such as "65536", "64KiB" or "1GiB".
func parseBytes(s string) (int64, error) {
	digits, scale := s, int64(1)
	for _, u := range byteUnits {
		if d, ok := strings.CutSuffix(s, u.suffix); ok {
			digits, scale = d, u.scale
			break
		}
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a byte count such as 65536 or 64MiB", s)
	}
	if n > math.MaxInt64/scale {
		return 0, fmt.Errorf("%q is too large", s)
	}
	return n * scale, nil
}
