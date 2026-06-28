package cmap

import (
	"strconv"
	"testing"
)

var sinkU32 uint32

func BenchmarkHash_Short(b *testing.B) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = strconv.Itoa(i % 9999)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkU32 = axhash(keys[i%1000])
	}
}

func BenchmarkHash_Medium(b *testing.B) {
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = "user-session-key-" + strconv.Itoa(i%1000)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sinkU32 = axhash(keys[i%1000])
	}
}

func TestAxhashDistribution(t *testing.T) {
	const keys = 100_000
	counts := make([]int, 32)
	for i := 0; i < keys; i++ {
		shard := axhash(strconv.Itoa(i)) & 31
		counts[shard]++
	}
	expected := keys / 32
	lo, hi := counts[0], counts[0]
	for _, c := range counts {
		if c < lo {
			lo = c
		}
		if c > hi {
			hi = c
		}
	}
	spread := float64(hi-lo) / float64(expected) * 100
	t.Logf("axhash: min=%d max=%d expected=%d spread=%.1f%%", lo, hi, expected, spread)
	if spread > 10 {
		t.Errorf("spread %.1f%% exceeds 10%% - poor distribution", spread)
	}
}
