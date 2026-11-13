package gen

import (
	"encoding/base64"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// jsonField starts a field: a comma unless it is the first, then its name.
func (e *encoder) jsonField(f *cfield, first *bool) {
	if !*first {
		e.b = append(e.b, ',')
	}
	*first = false
	e.b = append(e.b, f.jname...)
}

// jstr writes s as encoding/json writes a Go string:
//   - '"' and '\\' after a backslash;
//   - \b, \f, \n, \r and \t for those controls, \u00XX for the other
//     bytes below 0x20;
//   - '&', '<' and '>' as \u00XX, encoding/json's HTML escaping;
//   - U+2028 and U+2029 as \u2028 and \u2029;
//   - each byte of invalid UTF-8 as U+FFFD, the replacement character.
//
// The escapes are spelled out byte by byte: an editing tool once turned a
// written-out escape into the character it stands for.
func (e *encoder) jstr(s []byte) {
	e.b = append(e.b, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			i++
			switch {
			case c == '"' || c == '\\':
				e.b = append(e.b, '\\', c)
			case c == '\b':
				e.b = append(e.b, '\\', 'b')
			case c == '\f':
				e.b = append(e.b, '\\', 'f')
			case c == '\n':
				e.b = append(e.b, '\\', 'n')
			case c == '\r':
				e.b = append(e.b, '\\', 'r')
			case c == '\t':
				e.b = append(e.b, '\\', 't')
			case c < 0x20 || c == '&' || c == '<' || c == '>':
				e.b = append(e.b, '\\', 'u', '0', '0', hexLower[c>>4], hexLower[c&15])
			default:
				e.b = append(e.b, c)
			}
			continue
		}
		r, n := utf8.DecodeRune(s[i:])
		switch {
		case r == utf8.RuneError && n == 1:
			e.b = utf8.AppendRune(e.b, utf8.RuneError)
		case r == 0x2028 || r == 0x2029:
			e.b = append(e.b, '\\', 'u', '2', '0', '2', hexLower[r&15])
		default:
			e.b = append(e.b, s[i:i+n]...)
		}
		i += n
	}
	e.b = append(e.b, '"')
}

// jsonIdentity writes the identity as 16 lowercase hex digits, so every
// JSON value starts with {"id":" and them.
func (e *encoder) jsonIdentity() {
	e.b = append(e.b, '"')
	for i := range 16 {
		e.b = append(e.b, hexLower[e.id>>(60-4*i)&15])
	}
	e.b = append(e.b, '"')
}

func (e *encoder) jsonBase64(s []byte) {
	e.b = append(e.b, '"')
	e.b = base64.StdEncoding.AppendEncode(e.b, s)
	e.b = append(e.b, '"')
}

// jsonInt writes integers and bools; enums are written by name.
func (e *encoder) jsonInt(t Type, v int64) {
	if t == TypeBool {
		e.b = strconv.AppendBool(e.b, v == 1)
		return
	}
	e.b = strconv.AppendInt(e.b, v, 10)
}

// jsonDouble writes x as encoding/json does: the shortest decimal, in
// exponent form only below 1e-6 or from 1e21, with a one-digit exponent
// where it fits.
func (e *encoder) jsonDouble(x float64) {
	format := byte('f')
	if a := math.Abs(x); a != 0 && (a < 1e-6 || a >= 1e21) {
		format = 'e'
	}
	e.b = strconv.AppendFloat(e.b, x, format, -1, 64)
	if format == 'e' {
		// Turn e-09 into e-9.
		if n := len(e.b); n >= 4 && e.b[n-4] == 'e' && e.b[n-3] == '-' && e.b[n-2] == '0' {
			e.b[n-2] = e.b[n-1]
			e.b = e.b[:n-1]
		}
	}
}

// jsonTimestamp writes a time.Time: RFC 3339 with the fraction's trailing
// zeros trimmed.
func (e *encoder) jsonTimestamp(ms int64) {
	e.b = append(e.b, '"')
	e.b = time.UnixMilli(ms).UTC().AppendFormat(e.b, time.RFC3339Nano)
	e.b = append(e.b, '"')
}

// jsonDate writes a date as YYYY-MM-DD.
func (e *encoder) jsonDate(days int64) {
	e.b = append(e.b, '"')
	e.b = time.UnixMilli(days*day).UTC().AppendFormat(e.b, time.DateOnly)
	e.b = append(e.b, '"')
}

// jsonMoney writes {"currency_code":"USD","amount":"59.58"}: the amount is
// a decimal string, as for a Go struct with a string amount.
func (e *encoder) jsonMoney(cur []byte, minor int64) {
	e.b = append(e.b, `{"currency_code":`...)
	e.jstr(cur)
	e.b = append(e.b, `,"amount":"`...)
	a := uint64(minor)
	if minor < 0 {
		e.b = append(e.b, '-')
		a = -a
	}
	e.b = strconv.AppendUint(e.b, a/100, 10)
	e.b = append(e.b, '.', byte('0'+a%100/10), byte('0'+a%10), '"', '}')
}
