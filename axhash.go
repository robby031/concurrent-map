package cmap

import "math/bits"

const (
	axSec0 uint64 = 0x2d358dccaa6c78a5
	axSec1 uint64 = 0x8bb84b93962eacc9
	axSec2 uint64 = 0x4b33a62ed433d4a3
	axSec3 uint64 = 0x4d5a2da51de1aa47

	axPri0 uint64 = 0xa0761d6478bd642f
	axPri1 uint64 = 0xe7037ed1a0b428db
	axPri2 uint64 = 0x8ebc6af09c88c6e3
	axPri3 uint64 = 0x589965cc75374cc3

	axLenMul uint64 = 0x9E3779B97F4A7C15
)

var axSeed = axSec0 ^ axPri0

// axMul is folded_multiply: 128-bit product, XOR of high and low halves.
func axMul(x, y uint64) uint64 {
	hi, lo := bits.Mul64(x, y)
	return hi ^ lo
}

func axR4(s string, i int) uint64 {
	return uint64(s[i]) | uint64(s[i+1])<<8 | uint64(s[i+2])<<16 | uint64(s[i+3])<<24
}

func axR8(s string, i int) uint64 {
	return axR4(s, i) | axR4(s, i+4)<<32
}

// axhash is the entry point. Dispatch mirrors the original axhash-core
// tier structure but uses only compile-time constants as secrets.
func axhash(key string) uint32 {
	n := len(key)
	if n == 0 {
		return uint32(axSeed)
	}
	if n <= 16 {
		return uint32(axShort(key, n, axSeed))
	}
	rotated := bits.RotateLeft64(axSeed, -n)
	if n <= 32 {
		return uint32(axMid1732(key, n, rotated))
	}
	if n <= 64 {
		return uint32(axMid3364(key, n, rotated))
	}
	return uint32(axLong(key, n, rotated))
}

// axShort handles 1–16 bytes. Identical to the original scalar backend.
func axShort(key string, n int, acc uint64) uint64 {
	lm := uint64(n) * axLenMul
	if n >= 8 {
		lo := axR8(key, 0)
		hi := axR8(key, n-8)
		m0 := axMul(lo^axSec0, hi^axPri0^lm)
		return axMul(m0^acc, lm^axSec1)
	}
	if n == 4 {
		v := axR4(key, 0)
		lo := v | (v << 32)
		hi := bits.RotateLeft64(lo, 17) ^ uint64(n) ^ axSec0
		m0 := axMul(lo^axSec1^acc, hi^axPri1)
		return axMul(m0, lm^axPri0)
	}
	var lo uint64
	if n >= 5 {
		lo = axR4(key, 0) | axR4(key, n-4)<<32
	} else {
		c1 := uint64(key[0])
		c2 := uint64(key[n>>1])
		c3 := uint64(key[n-1])
		raw := c1 | c2<<8 | c3<<16
		lo = raw & ((uint64(1) << uint(n*8)) - 1)
	}
	hi := bits.RotateLeft64(lo, 17) ^ uint64(n) ^ axSec0
	m0 := axMul(lo^axSec1^acc, hi^axPri1)
	return axMul(m0, lm^axPri0)
}

// axMid1732 handles 17–32 bytes. Identical to the original scalar backend.
func axMid1732(key string, n int, acc uint64) uint64 {
	lm := uint64(n) * axLenMul
	a, b := axR8(key, 0), axR8(key, 8)
	c, d := axR8(key, n-16), axR8(key, n-8)
	front := axMul(a^axSec0, b^axPri0)
	back := axMul(c^axSec1, d^axPri1^lm)
	return axMul(front^bits.RotateLeft64(back, 17)^acc, lm^axSec2)
}

// axMid3364 handles 33–64 bytes. Identical to the original scalar backend.
func axMid3364(key string, n int, acc uint64) uint64 {
	lm := uint64(n) * axLenMul
	w0, w1 := axR8(key, 0), axR8(key, 8)
	w2, w3 := axR8(key, 16), axR8(key, 24)
	w4, w5 := axR8(key, n-32), axR8(key, n-24)
	w6, w7 := axR8(key, n-16), axR8(key, n-8)
	f0 := axMul(w0^axSec0, w1^axPri0)
	f1 := axMul(w2^axSec1, w3^axPri1)
	b0 := axMul(w4^axSec2, w5^axPri2^lm)
	b1 := axMul(w6^axSec3, w7^axPri3)
	front := f0 ^ bits.RotateLeft64(f1, 17)
	back := b0 ^ bits.RotateLeft64(b1, 21)
	m := axMul(front^acc, back^axPri0)
	return axMul(bits.RotateLeft64(m^lm, 23), axSec1^acc)
}

// axLong handles strings > 64 bytes.
// Replaces the original multi-lane stripe machinery with a simple rolling mix.
// Map keys this long are rare; uniform distribution is preserved.
func axLong(key string, n int, seed uint64) uint64 {
	i := 0
	for ; i+16 <= n; i += 16 {
		seed = axMul(axR8(key, i)^axSec0, axR8(key, i+8)^seed)
	}
	rem := n - i
	var a, b uint64
	switch {
	case rem >= 8:
		a = axR8(key, i)
		b = axR8(key, n-8)
	case rem >= 4:
		a = axR4(key, i) | axR4(key, n-4)<<32
	case rem > 0:
		a = uint64(key[i]) | uint64(key[i+rem>>1])<<8 | uint64(key[n-1])<<16
	}
	seed ^= axMul(a^axSec1, b^seed)
	return axMul(seed^axSec0, uint64(n)^axPri0)
}
