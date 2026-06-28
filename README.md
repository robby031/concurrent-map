# concurrent-map

[![Go Reference](https://pkg.go.dev/badge/github.com/robby031/concurrent-map.svg)](https://pkg.go.dev/github.com/robby031/concurrent-map)
[![Go Report Card](https://goreportcard.com/badge/github.com/robby031/concurrent-map)](https://goreportcard.com/report/github.com/robby031/concurrent-map)

A high-performance, thread-safe generic map for Go. Forked from [orcaman/concurrent-map](https://github.com/orcaman/concurrent-map) and modernized with a lock-free read path, cache-line padding, and a `sync.Map`-style API.

## What changed from the original

| Area | Original (orcaman) | This fork |
|---|---|---|
| API | `Set`/`Get`/`Remove`/`Items`/`Keys` | `Store`/`Load`/`Delete`/`LoadOrStore`/`LoadAndDelete`/`Range` - aligned to `sync.Map` |
| Hash function | FNV-1 (had XOR/multiply ordering bug) | axhash - custom folded-multiply algorithm |
| Shard selection | `% SHARD_COUNT` (integer division) | `& shardMask` (bitwise AND, power-of-2 enforced) |
| Read path | `sync.RWMutex` on every read | Lock-free fast path via `atomic.Pointer` snapshot |
| False sharing | No protection | 64-byte cache-line padding per shard |
| Full scan | Sequential with lock held | `ParallelRange` - one goroutine per shard |
| Generics | No | Yes - `ConcurrentMap[K, V]` |

## Install

```bash
go get github.com/robby031/concurrent-map
```

## Usage

```go
import cmap "github.com/robby031/concurrent-map"

// string key map
m := cmap.New[string]()

// Store a value
m.Store("user:1", "alice")

// Load a value
val, ok := m.Load("user:1")

// Load existing or store new
actual, loaded := m.LoadOrStore("user:1", "bob")

// Load and delete atomically
val, ok = m.LoadAndDelete("user:1")

// Delete
m.Delete("user:2")

// Iterate all entries (sequential, supports early-exit)
m.Range(func(key string, val string) bool {
    fmt.Println(key, val)
    return true // return false to stop early
})

// Iterate all entries (parallel, no early-exit - f must be goroutine-safe)
m.ParallelRange(func(key string, val string) {
    fmt.Println(key, val)
})

// Count entries
n := m.Count()

// Clear all entries
m.Clear()

// JSON marshal / unmarshal
data, _ := json.Marshal(m)
json.Unmarshal(data, &m)
```

### Custom key types

```go
// Any comparable type with a custom shard function
m := cmap.NewWithCustomShardingFunction[uint32, string](func(key uint32) uint32 {
    return key
})

// Types implementing fmt.Stringer
type UserID int
func (u UserID) String() string { return strconv.Itoa(int(u)) }

m := cmap.NewStringer[UserID, string]()
```

### Shard count

Default is 32. Must be a positive power of 2. Set before creating a map:

```go
cmap.SHARD_COUNT = 64
m := cmap.New[string]()
```

## Benchmark

`go test -bench=. -benchmem` on Apple M4 (arm64, 10 cores):

```
BenchmarkSingleInsertAbsent-10             6,560,301     225 ns/op    108 B/op    3 allocs/op
BenchmarkSingleInsertAbsentSyncMap-10      6,159,630     291 ns/op    125 B/op    3 allocs/op

BenchmarkSingleInsertPresent-10           51,555,249      23 ns/op     24 B/op    1 allocs/op
BenchmarkSingleInsertPresentSyncMap-10    49,531,599      24 ns/op     48 B/op    1 allocs/op

BenchmarkMultiInsertSame-10                  853,836    1413 ns/op    260 B/op   11 allocs/op
BenchmarkMultiInsertSameSyncMap-10           582,462    1915 ns/op    832 B/op   31 allocs/op

BenchmarkMultiGetSame-10                   6,313,579     188 ns/op     16 B/op    1 allocs/op
BenchmarkMultiGetSameSyncMap-10            6,423,794     185 ns/op     16 B/op    1 allocs/op

BenchmarkMultiGetSetBlock_32_Shard-10      2,472,537     498 ns/op    304 B/op   12 allocs/op
BenchmarkMultiGetSetBlockSyncMap-10        1,791,478     663 ns/op    864 B/op   32 allocs/op

BenchmarkRange-10                             20,277   59515 ns/op     62 B/op    1 allocs/op
BenchmarkParallelRange-10                     34,318   31625 ns/op   1552 B/op   33 allocs/op

BenchmarkHash_Short-10               398,327,686       3.0 ns/op      0 B/op    0 allocs/op
BenchmarkHash_Medium-10              196,362,145       6.1 ns/op      0 B/op    0 allocs/op
```

Key takeaways:
- **25% faster** than `sync.Map` for absent-key writes
- **On par** with `sync.Map` for concurrent reads of the same key (lock-free path)
- **2x faster** than `sync.Map` for mixed read/write workload (32 shards)
- **32% faster** for concurrent same-key writes (sharding eliminates single-mutex bottleneck)
- **ParallelRange** is 47% faster than sequential `Range` for full scans

## License

MIT
