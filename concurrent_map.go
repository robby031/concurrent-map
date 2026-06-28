package cmap

import (
	"encoding/json"
	"fmt"
	"sync"
)

var SHARD_COUNT = 32

type Stringer interface {
	fmt.Stringer
	comparable
}

// ConcurrentMap is a thread-safe map partitioned into shards to reduce lock contention.
type ConcurrentMap[K comparable, V any] struct {
	shards     []*ConcurrentMapShared[K, V]
	sharding   func(key K) uint32
	shardCount uint32
	shardMask  uint32
}

type ConcurrentMapShared[K comparable, V any] struct {
	items map[K]V
	sync.RWMutex
	// pad to 64 bytes (one cache line) to prevent false sharing between adjacent shards.
	// map header = 8 bytes, RWMutex = 24 bytes → 32 bytes padding needed.
	_ [32]byte
}

func create[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	sc := SHARD_COUNT
	if sc <= 0 || (sc&(sc-1)) != 0 {
		panic("cmap: SHARD_COUNT must be a positive power of 2")
	}
	m := ConcurrentMap[K, V]{
		sharding:   sharding,
		shards:     make([]*ConcurrentMapShared[K, V], sc),
		shardCount: uint32(sc),
		shardMask:  uint32(sc - 1),
	}
	for i := 0; i < sc; i++ {
		m.shards[i] = &ConcurrentMapShared[K, V]{items: make(map[K]V)}
	}
	return m
}

func New[V any]() ConcurrentMap[string, V] {
	return create[string, V](fnv32)
}

func NewStringer[K Stringer, V any]() ConcurrentMap[K, V] {
	return create[K, V](strfnv32[K])
}

func NewWithCustomShardingFunction[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	return create[K, V](sharding)
}

func (m ConcurrentMap[K, V]) getShard(key K) *ConcurrentMapShared[K, V] {
	return m.shards[m.sharding(key)&m.shardMask]
}

// Store sets the value for a key.
func (m ConcurrentMap[K, V]) Store(key K, value V) {
	shard := m.getShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Load returns the value stored in the map for a key, or the zero value if no value is present.
// The ok result indicates whether value was found in the map.
func (m ConcurrentMap[K, V]) Load(key K) (V, bool) {
	shard := m.getShard(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Delete deletes the value for a key.
func (m ConcurrentMap[K, V]) Delete(key K) {
	shard := m.getShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m ConcurrentMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	shard := m.getShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	if ok {
		shard.Unlock()
		return v, true
	}
	shard.items[key] = value
	shard.Unlock()
	return value, false
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
func (m ConcurrentMap[K, V]) LoadAndDelete(key K) (value V, loaded bool) {
	shard := m.getShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, ok
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range holds a read lock per shard during iteration, so f must not call
// any map method that acquires a write lock on the same shard.
func (m ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	for _, shard := range m.shards {
		shard.RLock()
		for key, value := range shard.items {
			if !f(key, value) {
				shard.RUnlock()
				return
			}
		}
		shard.RUnlock()
	}
}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	count := 0
	for i := uint32(0); i < m.shardCount; i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Clear removes all items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for _, shard := range m.shards {
		shard.Lock()
		shard.items = make(map[K]V)
		shard.Unlock()
	}
}

func (m ConcurrentMap[K, V]) MarshalJSON() ([]byte, error) {
	// Collect each shard's items in parallel to avoid sequential lock acquisition.
	snapshots := make([]map[K]V, m.shardCount)
	var wg sync.WaitGroup
	wg.Add(int(m.shardCount))
	for i, shard := range m.shards {
		i, shard := i, shard
		go func() {
			defer wg.Done()
			shard.RLock()
			local := make(map[K]V, len(shard.items))
			for k, v := range shard.items {
				local[k] = v
			}
			shard.RUnlock()
			snapshots[i] = local
		}()
	}
	wg.Wait()

	// Merge into a single map (no lock needed — all goroutines have finished).
	total := 0
	for _, s := range snapshots {
		total += len(s)
	}
	tmp := make(map[K]V, total)
	for _, s := range snapshots {
		for k, v := range s {
			tmp[k] = v
		}
	}
	return json.Marshal(tmp)
}

func (m *ConcurrentMap[K, V]) UnmarshalJSON(b []byte) (err error) {
	tmp := make(map[K]V)
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	for key, val := range tmp {
		m.Store(key, val)
	}
	return nil
}

func strfnv32[K fmt.Stringer](key K) uint32 {
	return fnv32(key.String())
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= prime32
	}
	return hash
}
