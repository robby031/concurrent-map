package cmap

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

func strAxhash[K fmt.Stringer](key K) uint32 {
	return axhash(key.String())
}

var SHARD_COUNT = 32

type Stringer interface {
	fmt.Stringer
	comparable
}

type entryVal[V any] struct {
	v        V
	expunged bool
}

type entry[V any] struct {
	p atomic.Pointer[entryVal[V]]
}

func newEntry[V any](v V) *entry[V] {
	e := &entry[V]{}
	e.p.Store(&entryVal[V]{v: v})
	return e
}

// load returns the value if the entry is live.
func (e *entry[V]) load() (V, bool) {
	l := e.p.Load()
	if l == nil || l.expunged {
		return *new(V), false
	}
	return l.v, true
}

// tryStore stores val without a mutex. Returns false when the entry is expunged
// and the caller must take the slow (locked) path.
func (e *entry[V]) tryStore(val V) bool {
	l := e.p.Load()
	if l != nil && l.expunged {
		return false
	}
	e.p.Store(&entryVal[V]{v: val})
	return true
}

// storeLocked stores val unconditionally. Caller must hold shard.mu.
func (e *entry[V]) storeLocked(val V) {
	e.p.Store(&entryVal[V]{v: val})
}

// tryDelete CAS-marks the entry as deleted. Returns (oldValue, true) on success.
func (e *entry[V]) tryDelete() (V, bool) {
	for {
		l := e.p.Load()
		if l == nil || l.expunged {
			return *new(V), false
		}
		if e.p.CompareAndSwap(l, nil) {
			return l.v, true
		}
	}
}

// tryExpungeLocked marks a nil entry as expunged. Caller must hold shard.mu.
func (e *entry[V]) tryExpungeLocked() bool {
	l := e.p.Load()
	for l == nil {
		if e.p.CompareAndSwap(nil, &entryVal[V]{expunged: true}) {
			return true
		}
		l = e.p.Load()
	}
	return false
}

// unexpungeLocked stores val, replacing an expunged entry with a live one.
// Caller must hold shard.mu. Returns true if the entry was expunged
// (meaning the caller must then add it back to dirty).
func (e *entry[V]) unexpungeLocked(val V) bool {
	l := e.p.Load()
	if l == nil || !l.expunged {
		return false
	}
	e.p.Store(&entryVal[V]{v: val})
	return true
}

// readOnly is the immutable snapshot stored in the shard's atomic pointer.
// The map itself is never modified; only individual entries (via atomic ops)
// change, so readers need no lock.
type readOnly[K comparable, V any] struct {
	m       map[K]*entry[V]
	amended bool // true when dirty has keys not in m
}

// ConcurrentMapShared is one cache-line-padded shard.
// Layout (64-bit): mu=8, read=8, dirty=8, misses=8 → 32 bytes → 32 pad → 64 total.
type ConcurrentMapShared[K comparable, V any] struct {
	mu     sync.Mutex
	read   atomic.Pointer[readOnly[K, V]]
	dirty  map[K]*entry[V]
	misses int
	_      [32]byte
}

// initDirtyLocked copies non-expunged entries from read into a fresh dirty map.
// Deleted entries (nil pointer) become expunged and are NOT copied.
// Caller must hold mu.
func (s *ConcurrentMapShared[K, V]) initDirtyLocked() {
	if s.dirty != nil {
		return
	}
	r := s.read.Load()
	s.dirty = make(map[K]*entry[V], len(r.m))
	for k, e := range r.m {
		if !e.tryExpungeLocked() {
			s.dirty[k] = e
		}
	}
}

// missLocked increments the miss counter and promotes dirty→read when the miss
// count reaches the dirty map size. Caller must hold mu.
func (s *ConcurrentMapShared[K, V]) missLocked() {
	s.misses++
	if s.dirty == nil || s.misses < len(s.dirty) {
		return
	}
	s.read.Store(&readOnly[K, V]{m: s.dirty})
	s.dirty = nil
	s.misses = 0
}

// promoteLocked forces dirty→read promotion. Caller must hold mu.
func (s *ConcurrentMapShared[K, V]) promoteLocked() {
	if s.dirty == nil {
		return
	}
	s.read.Store(&readOnly[K, V]{m: s.dirty})
	s.dirty = nil
	s.misses = 0
}

// Map
// ConcurrentMap is a thread-safe map partitioned into shards to reduce lock contention.
type ConcurrentMap[K comparable, V any] struct {
	shards     []*ConcurrentMapShared[K, V]
	sharding   func(key K) uint32
	shardCount uint32
	shardMask  uint32
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
		s := &ConcurrentMapShared[K, V]{}
		s.read.Store(&readOnly[K, V]{m: make(map[K]*entry[V])})
		m.shards[i] = s
	}
	return m
}

func New[V any]() ConcurrentMap[string, V] {
	return create[string, V](axhash)
}

func NewStringer[K Stringer, V any]() ConcurrentMap[K, V] {
	return create[K, V](strAxhash[K])
}

func NewWithCustomShardingFunction[K comparable, V any](sharding func(key K) uint32) ConcurrentMap[K, V] {
	return create[K, V](sharding)
}

func (m ConcurrentMap[K, V]) getShard(key K) *ConcurrentMapShared[K, V] {
	return m.shards[m.sharding(key)&m.shardMask]
}

// Operations
// Store sets the value for a key.
// Fast path (no mutex): key is already in the read snapshot and not expunged.
// Slow path (mutex): new key, or key was expunged and must be re-added to dirty.
func (m ConcurrentMap[K, V]) Store(key K, value V) {
	shard := m.getShard(key)

	read := shard.read.Load()
	if e, ok := read.m[key]; ok && e.tryStore(value) {
		return
	}

	shard.mu.Lock()
	read = shard.read.Load()
	if e, ok := read.m[key]; ok {
		if e.unexpungeLocked(value) {
			if shard.dirty == nil {
				shard.dirty = make(map[K]*entry[V])
			}
			shard.dirty[key] = e
		} else {
			e.storeLocked(value)
		}
	} else if e, ok := shard.dirty[key]; ok {
		e.storeLocked(value)
	} else {
		if !read.amended {
			shard.initDirtyLocked()
			shard.read.Store(&readOnly[K, V]{m: read.m, amended: true})
		}
		shard.dirty[key] = newEntry[V](value)
	}
	shard.mu.Unlock()
}

// Load returns the value stored in the map for a key, or the zero value if no
// value is present. The ok result indicates whether value was found in the map.
// Fast path (no mutex): key found in read snapshot.
// Slow path (mutex): key is in dirty only; increments miss counter.
func (m ConcurrentMap[K, V]) Load(key K) (V, bool) {
	shard := m.getShard(key)

	read := shard.read.Load()
	if e, ok := read.m[key]; ok {
		return e.load()
	}
	if !read.amended {
		return *new(V), false
	}

	shard.mu.Lock()
	// Re-check read under lock - dirty may have been promoted while we waited.
	read = shard.read.Load()
	e, ok := read.m[key]
	if !ok {
		e, ok = shard.dirty[key]
		shard.missLocked()
	}
	shard.mu.Unlock()

	if !ok {
		return *new(V), false
	}
	return e.load()
}

// Delete deletes the value for a key.
func (m ConcurrentMap[K, V]) Delete(key K) {
	m.LoadAndDelete(key)
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value.
// The loaded result is true if the value was loaded, false if stored.
func (m ConcurrentMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	shard := m.getShard(key)

	// Fast path: key is live in read snapshot.
	read := shard.read.Load()
	if e, ok := read.m[key]; ok {
		if v, vok := e.load(); vok {
			return v, true
		}
	}

	shard.mu.Lock()
	read = shard.read.Load()
	if e, ok := read.m[key]; ok {
		if v, vok := e.load(); vok {
			shard.mu.Unlock()
			return v, true
		}
		// Nil or expunged - store new value.
		if e.unexpungeLocked(value) {
			if shard.dirty == nil {
				shard.dirty = make(map[K]*entry[V])
			}
			shard.dirty[key] = e
		} else {
			e.storeLocked(value)
		}
	} else if e, ok := shard.dirty[key]; ok {
		if v, vok := e.load(); vok {
			shard.mu.Unlock()
			return v, true
		}
		e.storeLocked(value)
	} else {
		if !read.amended {
			shard.initDirtyLocked()
			shard.read.Store(&readOnly[K, V]{m: read.m, amended: true})
		}
		shard.dirty[key] = newEntry[V](value)
	}
	shard.mu.Unlock()
	return value, false
}

// LoadAndDelete deletes the value for a key, returning the previous value if any.
// The loaded result reports whether the key was present.
// Fast path (no mutex): key found in read snapshot and successfully CAS-deleted.
// Slow path (mutex): key is in dirty only, or read was amended.
func (m ConcurrentMap[K, V]) LoadAndDelete(key K) (V, bool) {
	shard := m.getShard(key)

	read := shard.read.Load()
	if e, ok := read.m[key]; ok {
		if v, deleted := e.tryDelete(); deleted {
			return v, true
		}
		if !read.amended {
			return *new(V), false
		}
	} else if !read.amended {
		return *new(V), false
	}

	shard.mu.Lock()
	read = shard.read.Load()
	var (
		e  *entry[V]
		ok bool
	)
	if e, ok = read.m[key]; ok {
		// Mark deleted in shared entry (affects both read and dirty).
		v, deleted := e.tryDelete()
		delete(shard.dirty, key)
		shard.mu.Unlock()
		return v, deleted
	}
	if shard.dirty != nil {
		e, ok = shard.dirty[key]
		delete(shard.dirty, key)
	}
	shard.mu.Unlock()

	if !ok {
		return *new(V), false
	}
	return e.load()
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, Range stops the iteration.
// Range promotes each shard's dirty map to read before iterating, then
// releases the lock - writers are not blocked during the iteration itself.
func (m ConcurrentMap[K, V]) Range(f func(key K, value V) bool) {
	for _, shard := range m.shards {
		shard.mu.Lock()
		shard.promoteLocked()
		r := shard.read.Load()
		shard.mu.Unlock()

		for k, e := range r.m {
			v, ok := e.load()
			if !ok {
				continue
			}
			if !f(k, v) {
				return
			}
		}
	}
}

// ParallelRange calls f concurrently - one goroutine per shard - for every
// key-value pair in the map. f must be safe for concurrent calls from multiple
// goroutines. Unlike Range, there is no early-exit.
func (m ConcurrentMap[K, V]) ParallelRange(f func(key K, value V)) {
	var wg sync.WaitGroup
	wg.Add(int(m.shardCount))
	for _, shard := range m.shards {
		shard := shard
		go func() {
			defer wg.Done()
			shard.mu.Lock()
			shard.promoteLocked()
			r := shard.read.Load()
			shard.mu.Unlock()

			for k, e := range r.m {
				if v, ok := e.load(); ok {
					f(k, v)
				}
			}
		}()
	}
	wg.Wait()
}

// Count returns the number of elements within the map.
func (m ConcurrentMap[K, V]) Count() int {
	total := 0
	for _, shard := range m.shards {
		shard.mu.Lock()
		var src map[K]*entry[V]
		if shard.dirty != nil {
			src = shard.dirty
		} else {
			src = shard.read.Load().m
		}
		for _, e := range src {
			if _, ok := e.load(); ok {
				total++
			}
		}
		shard.mu.Unlock()
	}
	return total
}

// Clear removes all items from map.
func (m ConcurrentMap[K, V]) Clear() {
	for _, shard := range m.shards {
		shard.mu.Lock()
		shard.read.Store(&readOnly[K, V]{m: make(map[K]*entry[V])})
		shard.dirty = nil
		shard.misses = 0
		shard.mu.Unlock()
	}
}

func (m ConcurrentMap[K, V]) MarshalJSON() ([]byte, error) {
	total := 0
	snapshots := make([]*readOnly[K, V], m.shardCount)
	for i, shard := range m.shards {
		shard.mu.Lock()
		shard.promoteLocked()
		snapshots[i] = shard.read.Load()
		total += len(snapshots[i].m)
		shard.mu.Unlock()
	}
	tmp := make(map[K]V, total)
	for _, r := range snapshots {
		for k, e := range r.m {
			if v, ok := e.load(); ok {
				tmp[k] = v
			}
		}
	}
	return json.Marshal(tmp)
}

func (m *ConcurrentMap[K, V]) UnmarshalJSON(b []byte) error {
	tmp := make(map[K]V)
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	for key, val := range tmp {
		m.Store(key, val)
	}
	return nil
}
