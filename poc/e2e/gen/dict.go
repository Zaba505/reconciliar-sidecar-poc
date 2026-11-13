package gen

import "strings"

var (
	syllables = strings.Split("ka.lo.mi.ne.ta.ri.so.ve.du.pa.ge.lu.zo.ban.der.fin.gor.hal.ix.jen.kor.lam.mor.nis.ost.per.qua.ris.sel.tur.ul.vin.wex.yal.zen", ".")
	suffixes  = []string{" Ltd", " Inc", " GmbH", " LLC", " plc", " SA", " BV", " AG", "", "", " & Co", " Holdings"}
	countries = []string{"GB", "DE", "FR", "NL", "ES", "IT", "BE", "IE", "US", "CH"}
)

// makeDict draws n dictionary entries of kind from r. The entries share
// one backing array.
func makeDict(r *rng, kind DictKind, n int) [][]byte {
	var b []byte
	word := func(lo, hi int) {
		for range lo + r.intn(hi-lo+1) {
			b = append(b, syllables[r.intn(len(syllables))]...)
		}
	}
	capWord := func(lo, hi int) {
		start := len(b)
		word(lo, hi)
		b[start] -= 'a' - 'A'
	}
	digits := func(n int) {
		for range n {
			b = append(b, byte('0'+r.intn(10)))
		}
	}
	letters := func(n int) {
		for range n {
			b = append(b, byte('A'+r.intn(26)))
		}
	}
	ends := make([]int, n)
	for i := range n {
		switch kind {
		case DictName:
			capWord(2, 3)
			if r.chance(0.4) {
				b = append(b, ' ')
				capWord(1, 3)
			}
			b = append(b, suffixes[r.intn(len(suffixes))]...)
		case DictIBAN:
			b = append(b, countries[r.intn(len(countries))]...)
			digits(2)
			letters(4)
			digits(10 + r.intn(9))
		case DictBIC:
			letters(4)
			b = append(b, countries[r.intn(len(countries))]...)
			letters(2)
			if r.chance(0.5) {
				b = append(b, "XXX"...)
			}
		case DictWord:
			word(1, 3)
		case DictUser:
			if r.chance(0.5) {
				b = append(b, "svc-"...)
				word(1, 2)
				b = append(b, '-')
				digits(2)
			} else {
				word(1, 2)
				b = append(b, '.')
				word(1, 2)
			}
		case DictTerminal:
			letters(2)
			digits(6)
		}
		ends[i] = len(b)
	}
	out := make([][]byte, n)
	start := 0
	for i, end := range ends {
		out[i] = b[start:end:end]
		start = end
	}
	return out
}
