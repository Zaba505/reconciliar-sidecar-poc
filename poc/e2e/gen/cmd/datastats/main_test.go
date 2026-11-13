package main

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"poc/e2e/gen"
	"poc/e2e/harness"
)

func TestDump_then_schema_makes_the_same_values(t *testing.T) {
	for _, p := range gen.Profiles() {
		var out bytes.Buffer
		if err := run([]string{"-profile", p.Name, "-dump"}, &out); err != nil {
			t.Fatalf("%s: -dump: %v", p.Name, err)
		}
		s, err := parseSchema(out.Bytes())
		if err != nil {
			t.Fatalf("%s: parsing its dump: %v\n%s", p.Name, err, out.Bytes())
		}
		for _, enc := range []gen.Encoding{gen.Protobuf, gen.Avro, gen.JSON} {
			want := gen.Config{Seed: 5, Keys: 1000, Left: gen.Values{Schema: p, Encoding: enc}, Right: gen.Values{Schema: p, Encoding: enc}}
			got := want
			got.Left.Schema, got.Right.Schema = s, s
			for k := range 200 {
				side := harness.Side(1 + k%2)
				if g, w := got.Value(side, k, k%3), want.Value(side, k, k%3); !bytes.Equal(g, w) {
					t.Fatalf("%s %s, key %d: the dumped schema's value is\n%x\nwant\n%x", p.Name, enc, k, g, w)
				}
			}
		}
	}
}

func TestParseSchema_rejects_bad_files(t *testing.T) {
	tests := map[string]string{
		"not JSON":        `{"name":`,
		"an unknown key":  `{"name":"X","fields":[{"name":"id","type":"identity","colour":"red"}]}`,
		"an unknown type": `{"name":"X","fields":[{"name":"id","type":"uuid"}]}`,
		"trailing data":   `{"name":"X","fields":[{"name":"id","type":"identity"}]} {}`,
		"no identity":     `{"name":"X","fields":[{"name":"n","type":"bool"}]}`,
	}
	for name, in := range tests {
		if _, err := parseSchema([]byte(in)); err == nil {
			t.Errorf("%s: parsed %s", name, in)
		}
	}
	if _, err := parseSchema([]byte(`{"name":"X","fields":[{"name":"id","type":"identity"},{"name":"n","type":"bool"}]}`)); err != nil {
		t.Errorf("a valid schema: %v", err)
	}
}

// varints frames vs as -framing varint does.
func varints(vs ...string) string {
	var b []byte
	for _, v := range vs {
		b = binary.AppendUvarint(b, uint64(len(v)))
		b = append(b, v...)
	}
	return string(b)
}

func TestReadValues(t *testing.T) {
	long := strings.Repeat("x", 300)
	tests := []struct {
		name, framing, in string
		want              []string
		fails             bool
	}{
		{"lines", "lines", "a\nbb\n\nccc", []string{"a", "bb", "", "ccc"}, false},
		{"lines with CRLF and a final newline", "lines", "a\r\nb\r\n", []string{"a", "b"}, false},
		{"no lines", "lines", "", nil, false},
		{"varints", "varint", varints("", "x", long), []string{"", "x", long}, false},
		{"no varints", "varint", "", nil, false},
		{"a cut length", "varint", varints("x") + "\x80", []string{"x"}, true},
		{"a cut value", "varint", varints("x") + varints(long)[:100], []string{"x"}, true},
		{"a huge length", "varint", varints("x") + string(binary.AppendUvarint(nil, maxValue+1)), []string{"x"}, true},
	}
	for _, tt := range tests {
		var err error
		var got []string
		for v := range readValues(strings.NewReader(tt.in), tt.framing, &err) {
			got = append(got, string(v))
		}
		if !slices.Equal(got, tt.want) || (err != nil) != tt.fails {
			t.Errorf("%s: got %q and error %v, want %q and an error: %v", tt.name, got, err, tt.want, tt.fails)
		}
	}
}

func TestRun_prints_a_row_for_each_sample(t *testing.T) {
	file := filepath.Join(t.TempDir(), "real.jsonl")
	if err := os.WriteFile(file, []byte(strings.Repeat(`{"a":1,"b":"two"}`+"\n", 100)), 0o644); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		args []string
		want [][]string // the first fields of each line
	}{
		{[]string{"-profile", "tiny", "-bytes", "16KiB"}, [][]string{{"profile", "encoding", "records"}, {"Tiny", "protobuf"}, {"Tiny", "avro"}, {"Tiny", "json"}}},
		{[]string{"-profile", "Tiny", "-encoding", "avro", "-side", "right", "-mix", "stress", "-keys", "decimal", "-bytes", "16KiB"}, [][]string{{"profile"}, {"Tiny", "avro"}}},
		{[]string{"-encoding", "JSON", "-bytes", "16KiB"}, [][]string{{"profile"}, {"Payment"}, {"BankLine"}, {"CardSettlement"}, {"Envelope"}, {"Tiny"}}},
		{[]string{"-values", file, "-framing", "lines"}, [][]string{{"file", "encoding"}, {"real.jsonl", "-", "100", "17.0", "17", "17", "17", "17"}}},
	}
	for _, tt := range tests {
		var out bytes.Buffer
		if err := run(tt.args, &out); err != nil {
			t.Errorf("%q: %v", tt.args, err)
			continue
		}
		lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
		ok := len(lines) == len(tt.want)
		for i := range min(len(lines), len(tt.want)) {
			f := strings.Fields(lines[i])
			ok = ok && len(f) >= len(tt.want[i]) && slices.Equal(f[:len(tt.want[i])], tt.want[i])
		}
		if !ok {
			t.Errorf("%q printed\n%s\nwant lines starting %q", tt.args, out.String(), tt.want)
		}
	}
}

func TestRun_rejects_conflicting_flags(t *testing.T) {
	for _, args := range [][]string{
		{"-profile", "Tiny", "-schema", "x.json"},
		{"-profile", "Tiny", "-values", "x"},
		{"-values", "x"},
		{"-values", "x", "-framing", "csv"},
		{"-framing", "lines"},
		{"-dump"},
		{"-dump", "-values", "x", "-framing", "lines"},
		{"-profile", "Unknown"},
		{"-profile", "Tiny", "-encoding", "xml"},
		{"-schema", "does-not-exist.json"},
		{"-profile", "Tiny", "stray"},
	} {
		if err := run(args, new(bytes.Buffer)); err == nil {
			t.Errorf("%q: no error", args)
		}
	}
}

func TestParseBytes(t *testing.T) {
	valid := map[string]int64{
		"0":     0,
		"65536": 64 << 10,
		"512B":  512,
		"4KiB":  4 << 10,
		"96MiB": 96 << 20,
		"2GiB":  2 << 30,
		"1TiB":  1 << 40,
	}
	for in, want := range valid {
		if got, err := parseBytes(in); err != nil || got != want {
			t.Errorf("parseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "-1", "1.5GiB", "1GB", "1 MiB", "16mib", "9223372036854775807KiB"} {
		if got, err := parseBytes(in); err == nil {
			t.Errorf("parseBytes(%q) = %d, want an error", in, got)
		}
	}
}
