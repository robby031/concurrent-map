package cmap

import (
	"encoding/json"
	"hash/fnv"
	"sort"
	"strconv"
	"testing"
)

type Animal struct {
	name string
}

func TestMapCreation(t *testing.T) {
	m := New[string]()
	if m.shards == nil {
		t.Error("map is null.")
	}
	if m.Count() != 0 {
		t.Error("new map should be empty.")
	}
}

func TestStore(t *testing.T) {
	m := New[Animal]()
	m.Store("elephant", Animal{"elephant"})
	m.Store("monkey", Animal{"monkey"})

	if m.Count() != 2 {
		t.Error("map should contain exactly two elements.")
	}
}

func TestLoad(t *testing.T) {
	m := New[Animal]()

	val, ok := m.Load("Money")
	if ok {
		t.Error("ok should be false when item is missing from map.")
	}
	if (val != Animal{}) {
		t.Error("Missing values should return as zero value.")
	}

	m.Store("elephant", Animal{"elephant"})

	elephant, ok := m.Load("elephant")
	if !ok {
		t.Error("ok should be true for item stored within the map.")
	}
	if elephant.name != "elephant" {
		t.Error("item was modified.")
	}
}

func TestDelete(t *testing.T) {
	m := New[Animal]()
	m.Store("monkey", Animal{"monkey"})
	m.Delete("monkey")

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was removed.")
	}

	temp, ok := m.Load("monkey")
	if ok {
		t.Error("Expecting ok to be false for missing items.")
	}
	if (temp != Animal{}) {
		t.Error("Expecting item to be zero value after deletion.")
	}

	// Delete a non-existing element should not panic.
	m.Delete("noone")
}

func TestLoadOrStore(t *testing.T) {
	m := New[Animal]()
	elephant := Animal{"elephant"}
	monkey := Animal{"monkey"}

	actual, loaded := m.LoadOrStore("elephant", elephant)
	if loaded {
		t.Error("should not be loaded on first call")
	}
	if actual != elephant {
		t.Error("returned value should be the stored value")
	}

	actual, loaded = m.LoadOrStore("elephant", monkey)
	if !loaded {
		t.Error("should be loaded on second call")
	}
	if actual != elephant {
		t.Error("returned value should be the original stored value, not the new one")
	}
}

func TestLoadAndDelete(t *testing.T) {
	m := New[Animal]()
	monkey := Animal{"monkey"}
	m.Store("monkey", monkey)

	v, loaded := m.LoadAndDelete("monkey")
	if !loaded || v != monkey {
		t.Error("LoadAndDelete didn't find a monkey.")
	}

	v2, loaded2 := m.LoadAndDelete("monkey")
	if loaded2 || v2 == monkey {
		t.Error("LoadAndDelete keeps finding monkey")
	}

	if m.Count() != 0 {
		t.Error("Expecting count to be zero once item was LoadAndDeleted.")
	}
}

func TestRange(t *testing.T) {
	m := New[Animal]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	counter := 0
	m.Range(func(key string, v Animal) bool {
		if (v == Animal{}) {
			t.Error("Expecting an object.")
		}
		counter++
		return true
	})

	if counter != 100 {
		t.Errorf("We should have counted 100 elements, got %d.", counter)
	}
}

func TestRangeEarlyExit(t *testing.T) {
	m := New[Animal]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}

	counter := 0
	m.Range(func(key string, v Animal) bool {
		counter++
		return counter < 10
	})

	if counter > 10 {
		t.Errorf("Range should have stopped early, got %d iterations.", counter)
	}
}

func TestCount(t *testing.T) {
	m := New[Animal]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	if m.Count() != 100 {
		t.Error("Expecting 100 element within map.")
	}
}

func TestClear(t *testing.T) {
	m := New[Animal]()
	for i := 0; i < 100; i++ {
		m.Store(strconv.Itoa(i), Animal{strconv.Itoa(i)})
	}
	m.Clear()
	if m.Count() != 0 {
		t.Error("We should have 0 elements.")
	}
}

func TestConcurrent(t *testing.T) {
	m := New[int]()
	ch := make(chan int)
	const iterations = 1000
	var a [iterations]int

	go func() {
		for i := 0; i < iterations/2; i++ {
			m.Store(strconv.Itoa(i), i)
			val, _ := m.Load(strconv.Itoa(i))
			ch <- val
		}
	}()

	go func() {
		for i := iterations / 2; i < iterations; i++ {
			m.Store(strconv.Itoa(i), i)
			val, _ := m.Load(strconv.Itoa(i))
			ch <- val
		}
	}()

	counter := 0
	for elem := range ch {
		a[counter] = elem
		counter++
		if counter == iterations {
			break
		}
	}

	sort.Ints(a[0:iterations])

	if m.Count() != iterations {
		t.Error("Expecting 1000 elements.")
	}

	for i := 0; i < iterations; i++ {
		if i != a[i] {
			t.Error("missing value", i)
		}
	}
}

func TestJsonMarshal(t *testing.T) {
	SHARD_COUNT = 2
	defer func() { SHARD_COUNT = 32 }()
	expected := "{\"a\":1,\"b\":2}"
	m := New[int]()
	m.Store("a", 1)
	m.Store("b", 2)
	j, err := json.Marshal(m)
	if err != nil {
		t.Error(err)
	}
	if string(j) != expected {
		t.Errorf("json %s differ from expected %s", string(j), expected)
	}
}

func TestFnv32(t *testing.T) {
	key := []byte("ABC")

	hasher := fnv.New32()
	_, err := hasher.Write(key)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
	if fnv32(string(key)) != hasher.Sum32() {
		t.Errorf("Bundled fnv32 produced %d, expected result from hash/fnv32 is %d", fnv32(string(key)), hasher.Sum32())
	}
}
