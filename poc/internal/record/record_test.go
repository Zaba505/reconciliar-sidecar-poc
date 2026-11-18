package record

import (
	"bytes"
	"testing"

	"pgregory.net/rapid"
)

func TestEncoding_round_trips(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOfN(rapid.Byte(), 0, 300).Draw(t, "key")
		value := rapid.SliceOfN(rapid.Byte(), 0, 1000).Draw(t, "value")
		side := Side(rapid.IntRange(0, 1).Draw(t, "side"))
		hash := Hash(key)
		rec := Append([]byte("prefix"), hash, side, key, value)[6:]
		if len(rec) != Len(len(key), len(value)) {
			t.Fatalf("encoded %d bytes, Len says %d", len(rec), Len(len(key), len(value)))
		}
		if n, ok := Size(rec); !ok || n != len(rec) {
			t.Fatalf("Size = %d, %v; want %d", n, ok, len(rec))
		}
		h, ok := ParseHeader(rec)
		if !ok || h.Hash != hash || h.Side != side || h.KeyLen != len(key) || h.ValLen != len(value) || h.Size() != len(rec) {
			t.Fatalf("ParseHeader = %+v, %v", h, ok)
		}
		gh, gs, gk, gv := Decode(rec)
		if gh != hash || gs != side || !bytes.Equal(gk, key) || !bytes.Equal(gv, value) {
			t.Fatalf("Decode = %x %v %q %q", gh, gs, gk, gv)
		}
		if s, k := SideKey(rec[:h.Len+h.KeyLen]); s != side || !bytes.Equal(k, key) {
			t.Fatalf("SideKey of the header and key = %v %q", s, k)
		}
		// A truncated header is not a header.
		for n := range h.Len {
			if _, ok := ParseHeader(rec[:n]); ok {
				t.Fatalf("ParseHeader of %d of the header's %d bytes succeeded", n, h.Len)
			}
		}
	})
}

func TestHash_of_an_empty_key_is_0_and_sorts_first(t *testing.T) {
	if Hash(nil) != 0 || Hash([]byte{}) != 0 {
		t.Error("an empty key's hash isn't 0")
	}
	empty := Append(nil, 0, Right, nil, []byte("v"))
	zero := Append(nil, 0, Left, []byte{0}, nil) // a key whose hash, if it were 0, ties
	if Compare(empty, zero) >= 0 {
		t.Error("an empty key doesn't sort before a non-empty key of the same hash")
	}
}

func TestCompare_orders_by_hash_key_then_side(t *testing.T) {
	a := Append(nil, 1, Right, []byte("b"), nil)
	b := Append(nil, 2, Left, []byte("a"), nil)
	c := Append(nil, 2, Left, []byte("b"), nil)
	d := Append(nil, 2, Right, []byte("b"), nil)
	recs := [][]byte{a, b, c, d}
	for i := range recs {
		for j := range recs {
			want := 0
			if i < j {
				want = -1
			} else if i > j {
				want = 1
			}
			if got := Compare(recs[i], recs[j]); got != want {
				t.Errorf("Compare(%d, %d) = %d, want %d", i, j, got, want)
			}
		}
	}
}

func TestFence_is_the_hash_top_byte(t *testing.T) {
	if Fence(0) != 0 || Fence(^uint64(0)) != Fences-1 || Fence(1<<56) != 1 {
		t.Error("fences aren't the hash's top byte")
	}
}
