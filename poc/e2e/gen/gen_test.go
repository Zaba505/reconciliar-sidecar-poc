package gen

import (
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	"poc/e2e/harness"
	reconv1 "poc/gen/recon/v1"
	"poc/internal/oracle"
)

func expected(c Config) []*reconv1.JoinResult {
	var records []oracle.Record
	for r := range c.Records(0, 1) {
		records = append(records, oracle.Record{Side: oracle.Side(r.Side - 1), Key: r.Key, Value: r.Value})
	}
	return oracle.Join(records)
}

func verify(c Config, results []*reconv1.JoinResult) error {
	v := NewVerifier(c)
	for _, r := range results {
		if err := v.Add(r); err != nil {
			return err
		}
	}
	return v.Finish()
}

func TestRecords_partition_the_run(t *testing.T) {
	c := Config{Seed: 1, Keys: 5000, ValueSize: 20}
	var whole, parts []string
	for r := range c.Records(0, 1) {
		whole = append(whole, string(r.Key)+string(r.Value))
	}
	var left, right uint64
	for p := range 3 {
		for r := range c.Records(p, 3) {
			parts = append(parts, string(r.Key)+string(r.Value))
			if r.Side == harness.Left {
				left++
			} else {
				right++
			}
		}
	}
	slices.Sort(whole)
	slices.Sort(parts)
	if !slices.Equal(whole, parts) {
		t.Error("the parts don't hold the run's records")
	}
	if l, r := c.Totals(); l != left || r != right || l == 0 || r == 0 {
		t.Errorf("Totals = %d, %d; the records have %d, %d", l, r, left, right)
	}
}

func TestVerifier_accepts_the_oracle_and_rejects_mistakes(t *testing.T) {
	c := Config{Seed: 2, Keys: 3000, ValueSize: 16}
	results := expected(c)
	if err := verify(c, results); err != nil {
		t.Fatalf("the oracle's results: %v", err)
	}

	var oneSided, match int = -1, -1
	for i, r := range results {
		if r.GetBoth() != nil && match < 0 && len(r.GetBoth().GetLeft()) > 1 {
			match = i
		}
		if r.GetLeftOnly() != nil && oneSided < 0 {
			oneSided = i
		}
	}
	mistakes := map[string]func([]*reconv1.JoinResult) []*reconv1.JoinResult{
		"a missing result":   func(rs []*reconv1.JoinResult) []*reconv1.JoinResult { return slices.Delete(rs, oneSided, oneSided+1) },
		"a duplicate result": func(rs []*reconv1.JoinResult) []*reconv1.JoinResult { return append(rs, rs[oneSided]) },
		"a missing match value": func(rs []*reconv1.JoinResult) []*reconv1.JoinResult {
			m := rs[match].GetBoth()
			m.Left = m.Left[1:]
			return rs
		},
		"a changed value": func(rs []*reconv1.JoinResult) []*reconv1.JoinResult {
			rs[oneSided].GetLeftOnly().Value[valueHeader] ^= 1
			return rs
		},
		"a wrong state": func(rs []*reconv1.JoinResult) []*reconv1.JoinResult {
			rec := rs[oneSided].GetLeftOnly()
			rs[oneSided] = &reconv1.JoinResult{Result: &reconv1.JoinResult_RightOnly{RightOnly: rec}}
			return rs
		},
	}
	for name, mistake := range mistakes {
		rs := make([]*reconv1.JoinResult, len(results))
		for i, r := range results {
			rs[i] = proto.Clone(r).(*reconv1.JoinResult)
		}
		if err := verify(c, mistake(rs)); err == nil {
			t.Errorf("%s went unnoticed", name)
		}
	}
}

func TestFingerprint_ignores_order_only(t *testing.T) {
	c := Config{Seed: 3, Keys: 2000, ValueSize: 12}
	results := expected(c)
	var a, b, d Fingerprint
	for _, r := range results {
		a.Add(r)
	}
	for _, r := range slices.Backward(results) {
		b.Add(r)
	}
	if a != b {
		t.Errorf("fingerprints differ by order: %+v, %+v", a, b)
	}
	for _, r := range results[1:] {
		d.Add(r)
	}
	if a == d || a.Count != uint64(len(results)) {
		t.Errorf("fingerprint %+v doesn't change without a result (%+v)", a, d)
	}
}
