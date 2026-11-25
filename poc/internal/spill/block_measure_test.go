package spill

import (
	"cmp"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"poc/e2e/gen"
	"poc/e2e/harness"
	"poc/internal/record"
)

// TestBlocks_measure writes realistic records, sorted as a spill writes
// them, as files of blocks of each codec and size, and logs the ratio of
// their records to their bytes, and how fast one core compresses and
// decodes their blocks. It runs only when RECON_BLOCK_BENCH_MIB is set:
//
//   - RECON_BLOCK_BENCH_MIB: MiB of key and value bytes, both sides.
//   - RECON_BLOCK_BENCH_DATA: left/right profiles and the encoding, as
//     Payment/BankLine/protobuf, the default.
func TestBlocks_measure(t *testing.T) {
	mib, _ := strconv.Atoi(os.Getenv("RECON_BLOCK_BENCH_MIB"))
	if mib <= 0 {
		t.Skip("set RECON_BLOCK_BENCH_MIB to the MiB of records to measure")
	}
	data := strings.Split(cmp.Or(os.Getenv("RECON_BLOCK_BENCH_DATA"), "Payment/BankLine/protobuf"), "/")
	enc, err := gen.ParseEncoding(data[2])
	if err != nil {
		t.Fatal(err)
	}
	cfg := gen.Config{
		Seed:      20,
		Left:      gen.Values{Schema: gen.Profile(data[0]), Encoding: enc},
		Right:     gen.Values{Schema: gen.Profile(data[1]), Encoding: enc},
		Mix:       gen.Typical,
		KeyFormat: gen.RFKeys,
	}
	cfg.Keys = cfg.KeysFor(int64(mib) << 20)
	var recs [][]byte
	for r := range cfg.Records(0, 1) {
		side := record.Left
		if r.Side != harness.Left {
			side = record.Right
		}
		recs = append(recs, record.Append(nil, record.Hash(r.Key), side, r.Key, r.Value))
	}
	slices.SortStableFunc(recs, record.Compare)

	fsys, dir := newHost(t)
	rows := []string{"| Codec | Block | Records / file bytes | Raw blocks | Compress MiB/s | Decode MiB/s |", "|---|---|---|---|---|---|"}
	for _, codec := range []Codec{S2, Zstd} {
		for _, block := range []int{4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10} {
			format := Format{Codec: codec, Block: block}
			f := writeAs(t, fsys, filepath.Join(dir, fmt.Sprintf("%v-%d.run", codec, block)), 1<<20, format, recs)
			file, err := os.ReadFile(f.Path)
			if err != nil {
				t.Fatal(err)
			}
			// Each block's records, and the body and size of each compressed one.
			e, d := NewEncoder(format), NewDecoder([]*File{f})
			var raws, bodies [][]byte
			var sizes []int
			stored := 0
			for at := 0; at < len(file); {
				kind, raw, n := parseBlockHeader(file[at:])
				body := file[at+blockHeader : at+blockHeader+int(n)]
				at += blockHeader + int(n)
				switch kind {
				case blockRaw:
					stored++
					raws = append(raws, body)
				case blockCompressed:
					dst := make([]byte, raw)
					if err := d.decode(codec, dst, body); err != nil {
						t.Fatal(err)
					}
					raws, bodies, sizes = append(raws, dst), append(bodies, body), append(sizes, int(raw))
				}
			}
			var rawBytes, decoded int
			for _, r := range raws {
				rawBytes += len(r)
			}
			start := time.Now()
			for _, r := range raws {
				e.compress(r)
			}
			compress := time.Since(start)
			dst := make([]byte, block)
			start = time.Now()
			for i, b := range bodies {
				if err := d.decode(codec, dst[:sizes[i]], b); err != nil {
					t.Fatal(err)
				}
				decoded += sizes[i]
			}
			decode := time.Since(start)
			rows = append(rows, fmt.Sprintf("| %v | %d KiB | %.2f | %d of %d | %.0f | %.0f |", codec, block>>10, float64(f.Raw)/float64(f.Bytes), stored, len(raws),
				float64(rawBytes)/(1<<20)/compress.Seconds(), float64(decoded)/(1<<20)/decode.Seconds()))
			d.Close()
			_ = os.Remove(f.Path)
		}
	}
	var raw int
	for _, r := range recs {
		raw += len(r)
	}
	t.Logf("%s, %d MiB of records:\n%s", strings.Join(data, "/"), raw>>20, strings.Join(rows, "\n"))
}
