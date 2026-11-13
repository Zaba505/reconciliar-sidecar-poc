package gen

import (
	"encoding/binary"
	"math"
)

// avroIdentity writes the identity as fixed Id(8). It is the first field,
// so every Avro value starts with the identity's 8 little-endian bytes.
func (e *encoder) avroIdentity() {
	e.b = binary.LittleEndian.AppendUint64(e.b, e.id)
}

// avroBytes writes a string or bytes: the length, then the content.
func (e *encoder) avroBytes(s []byte) {
	e.uv(zigzag(int64(len(s))))
	e.b = append(e.b, s...)
}

// avroInt writes int, long, boolean and enum values; an enum is its index.
func (e *encoder) avroInt(t Type, v int64) {
	if t == TypeBool {
		e.b = append(e.b, byte(v))
		return
	}
	e.uv(zigzag(v))
}

func (e *encoder) avroDouble(x float64) {
	e.b = binary.LittleEndian.AppendUint64(e.b, math.Float64bits(x))
}

// avroMoney writes the record {string currency_code; bytes amount}, where
// the amount is a decimal at scale 2: the minor units as minimal
// big-endian two's complement.
func (e *encoder) avroMoney(cur []byte, minor int64) {
	e.avroBytes(cur)
	var be [8]byte
	binary.BigEndian.PutUint64(be[:], uint64(minor))
	i := 0
	for i < 7 && (be[i] == 0 && be[i+1]&0x80 == 0 || be[i] == 0xff && be[i+1]&0x80 != 0) {
		i++
	}
	e.avroBytes(be[i:])
}
