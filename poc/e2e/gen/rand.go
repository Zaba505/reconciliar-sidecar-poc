package gen

import (
	"math"
	"math/bits"
)

// rng is wyrand. Each value gets its own, seeded from its identity.
type rng struct{ s uint64 }

func (r *rng) next() uint64 {
	r.s += 0xa0761d6478bd642f
	hi, lo := bits.Mul64(r.s, r.s^0xe7037ed1a0b428db)
	return hi ^ lo
}

// intn returns a number in [0, n), by Lemire's multiply-shift.
func (r *rng) intn(n int) int {
	hi, _ := bits.Mul64(r.next(), uint64(n))
	return int(hi)
}

// uint64n returns a number in [0, n).
func (r *rng) uint64n(n uint64) uint64 {
	hi, _ := bits.Mul64(r.next(), n)
	return hi
}

func (r *rng) float() float64        { return float64(r.next()>>11) / (1 << 53) }
func (r *rng) chance(p float64) bool { return p > 0 && r.float() < p }

// norm returns a standard normal number, by Box-Muller.
func (r *rng) norm() float64 {
	u1 := max(r.float(), 1e-300)
	return math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*r.float())
}

// lognormal returns a lognormal number around median.
func (r *rng) lognormal(median, sigma float64) float64 {
	return median * math.Exp(sigma*r.norm())
}

// length draws a length from d.
func (r *rng) length(d Dist) int {
	v := d.Median
	if d.Sigma > 0 {
		v = r.lognormal(d.Median, d.Sigma)
	}
	return int(min(max(math.Round(v), float64(d.Min)), float64(d.Max)))
}

// toInt rounds x to an integer, clamped to ±2^62 so that the conversion is
// the same on every platform.
func toInt(x float64) int64 {
	const lim = 1 << 62
	return int64(min(max(math.Round(x), -lim), lim))
}

// alias picks index i with probability proportional to its weight in O(1),
// by Walker's alias method.
type alias struct {
	cut   []uint32
	alias []int32
}

func newWeighted(w []float64) alias {
	n := len(w)
	w = append([]float64(nil), w...)
	var sum float64
	for _, x := range w {
		sum += x
	}
	z := alias{cut: make([]uint32, n), alias: make([]int32, n)}
	var small, large []int
	for i := range w {
		w[i] *= float64(n) / sum
		if w[i] < 1 {
			small = append(small, i)
		} else {
			large = append(large, i)
		}
	}
	for len(small) > 0 && len(large) > 0 {
		l, g := small[len(small)-1], large[len(large)-1]
		small = small[:len(small)-1]
		z.cut[l] = uint32(w[l] * (1 << 32))
		z.alias[l] = int32(g)
		w[g] -= 1 - w[l]
		if w[g] < 1 {
			large = large[:len(large)-1]
			small = append(small, g)
		}
	}
	for _, i := range append(small, large...) {
		z.cut[i], z.alias[i] = math.MaxUint32, int32(i)
	}
	return z
}

// newUniform returns an alias table of n equal weights.
func newUniform(n int) alias {
	w := make([]float64, n)
	for i := range w {
		w[i] = 1
	}
	return newWeighted(w)
}

// newZipf returns an alias table that picks i with weight 1/(i+1)^s.
func newZipf(n int, s float64) alias {
	w := make([]float64, n)
	for i := range w {
		w[i] = 1 / math.Pow(float64(i+1), s)
	}
	return newWeighted(w)
}

func (z *alias) pick(r *rng) int {
	x := r.next()
	i := int((x >> 32) * uint64(len(z.cut)) >> 32)
	if uint32(x) < z.cut[i] {
		return i
	}
	return int(z.alias[i])
}

// mix is SplitMix64's finalizer, a bijection of 64-bit numbers.
func mix(z uint64) uint64 {
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// unmix inverts mix.
func unmix(z uint64) uint64 {
	z = unxorshift(z, 31)
	z *= 0x319642b2d24d8ec3 // the inverse of 0x94d049bb133111eb
	z = unxorshift(z, 27)
	z *= 0x96de1b173f119089 // the inverse of 0xbf58476d1ce4e5b9
	return unxorshift(z, 30)
}

// unxorshift inverts z ^ z>>s.
func unxorshift(z uint64, s uint) uint64 {
	x := z
	for i := s; i < 64; i += s {
		x = z ^ x>>s
	}
	return x
}
