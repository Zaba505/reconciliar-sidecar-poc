package gen

import (
	"encoding/binary"
	"math"
	"time"
)

// Protobuf wire types.
const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
)

func (e *encoder) tag(num, wire int) { e.uv(uint64(num)<<3 | uint64(wire)) }
func (e *encoder) uv(v uint64)       { e.b = binary.AppendUvarint(e.b, v) }

// open starts a length-delimited protobuf value, a message or a packed
// list, at depth: what follows goes to that depth's buffer until close.
// It returns the buffer to restore.
func (e *encoder) open(depth int) (parent []byte) {
	for len(e.nest) <= depth {
		e.nest = append(e.nest, nil)
	}
	parent = e.b
	e.b = e.nest[depth][:0]
	return parent
}

// close ends the value open started and appends it to parent as field
// num, unless it is empty and omitEmpty is set.
func (e *encoder) close(depth int, parent []byte, num int, omitEmpty bool) {
	child := e.b
	e.nest[depth] = child
	e.b = parent
	if omitEmpty && len(child) == 0 {
		return
	}
	e.tag(num, wireBytes)
	e.uv(uint64(len(child)))
	e.b = append(e.b, child...)
}

// protoIdentity writes the identity as fixed64. It is the first field, so
// every protobuf value starts with the tag 0x09 and the identity's 8
// little-endian bytes.
func (e *encoder) protoIdentity(num int) {
	e.tag(num, wireFixed64)
	e.b = binary.LittleEndian.AppendUint64(e.b, e.id)
}

func (e *encoder) protoBytes(num int, m mode, s []byte) {
	if len(s) == 0 && m == implicit {
		return
	}
	e.tag(num, wireBytes)
	e.uv(uint64(len(s)))
	e.b = append(e.b, s...)
}

// protoInt writes int32, int64, sint64, bool and enum values. Enum index i
// is written as i+1, because 0 is X_UNSPECIFIED.
func (e *encoder) protoInt(t Type, num int, m mode, v int64) {
	pv := uint64(v)
	switch t {
	case TypeSInt64:
		pv = zigzag(v)
	case TypeEnum:
		pv = uint64(v + 1)
	case TypeInt32:
		pv = uint64(int64(int32(v)))
	}
	if pv == 0 && m == implicit {
		return
	}
	if m != packedElem {
		e.tag(num, wireVarint)
	}
	e.uv(pv)
}

func (e *encoder) protoDouble(num int, m mode, x float64) {
	bits := math.Float64bits(x)
	if bits == 0 && m == implicit {
		return
	}
	if m != packedElem {
		e.tag(num, wireFixed64)
	}
	e.b = binary.LittleEndian.AppendUint64(e.b, bits)
}

// protoTimestamp writes a google.protobuf.Timestamp, whose nanos are never
// negative: a time before 1970 has seconds rounded down.
func (e *encoder) protoTimestamp(num, depth int, ms int64) {
	s, n := ms/1000, ms%1000*1e6
	if n < 0 {
		s, n = s-1, n+1e9
	}
	parent := e.open(depth)
	if s != 0 {
		e.tag(1, wireVarint)
		e.uv(uint64(s))
	}
	if n != 0 {
		e.tag(2, wireVarint)
		e.uv(uint64(n))
	}
	e.close(depth, parent, num, false)
}

// protoDate writes a message shaped like google.type.Date.
func (e *encoder) protoDate(num, depth int, days int64) {
	y, mo, d := time.UnixMilli(days * day).UTC().Date()
	parent := e.open(depth)
	for i, v := range [3]int{y, int(mo), d} {
		if v != 0 {
			e.tag(i+1, wireVarint)
			e.uv(uint64(int64(int32(v))))
		}
	}
	e.close(depth, parent, num, false)
}

// protoMoney writes a message shaped like google.type.Money: units and
// nanos have the amount's sign.
func (e *encoder) protoMoney(num, depth int, cur []byte, minor int64) {
	parent := e.open(depth)
	if len(cur) > 0 {
		e.tag(1, wireBytes)
		e.uv(uint64(len(cur)))
		e.b = append(e.b, cur...)
	}
	if u := minor / 100; u != 0 {
		e.tag(2, wireVarint)
		e.uv(uint64(u))
	}
	if n := minor % 100 * 10_000_000; n != 0 {
		e.tag(3, wireVarint)
		e.uv(uint64(int64(int32(n))))
	}
	e.close(depth, parent, num, false)
}
