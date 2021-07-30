package txncache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestCacheBasic(t *testing.T) {
	cache, _ := NewCache(GetValue, GetMultiValue, 500)
	defer cache.Close()
	keyCount := 1000
	t.Logf("Max key count: %v", keyCount)
	keys := make([]Key, 0)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, ZKey(fmt.Sprintf("k%v", i)))
	}
	res := cache.MultiGet(keys)
	if len(res) != len(keys) {
		t.Error("Less result fetched")
	}
	for k, v := range res {
		fmt.Printf("%v - %v\n", k, v)
	}
	t.Logf("%v results fetched for %v keys", len(res), len(keys))
}

func TestCacheDefaultAndPreload(t *testing.T) {
	cache, _ := NewCache(GetValue, GetMultiValue, 500)
	defer cache.Close()
	cache.DefaultValue("v###")
	cache.Preload(map[string]Value{
		"k1": "v#k1#",
	})
	keyCount := 10000
	t.Logf("Max key count: %v", keyCount)
	keys := make([]Key, 0)
	for i := 0; i < keyCount; i++ {
		keys = append(keys, ZKey(fmt.Sprintf("k%v", i)))
	}
	res := cache.MultiGet(keys)
	if len(res) != len(keys) {
		t.Error("Less result fetched")
	}
	for k, v := range res {
		if k.ID() == "k1" && v != "v#k1#" {
			t.Errorf("wrong value of key k1 %v", v)
		}
		if k.ID() == "k2" && v != "v###" {
			t.Errorf("wrong value of key k2 %v", v)
		}
	}
}

func TestCacheParallel(t *testing.T) {
	count := &sync.Map{}
	fmt.Println("started")
	preload := map[string]Value{
		"k1": "v#k1#",
	}
	cache, _ := NewCache(GetValue, GetMultiValue, 500)
	cache.CloseAfter(10 * time.Millisecond)
	cache.Preload(preload)
	cache.DefaultValue("v###")
	cache.CommonArgs(count)
	defer cache.Close()
	var wg sync.WaitGroup
	rand.Seed(time.Now().Unix())
	keyCount := rand.Intn(3000)
	t.Logf("Max key count: %v", keyCount)
	requests := rand.Intn(100)
	requestList := make([][]Key, requests)
	for i := 0; i < requests; i++ {
		keys := make([]Key, 0)
		for i := 0; i < keyCount; i++ {
			if rand.Intn(100) < 50 {
				keys = append(keys, ZKey(fmt.Sprintf("k%v", i)))
			}
		}
		requestList[i] = keys
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := cache.MultiGet(keys)
			if len(res) != len(keys) {
				t.Error("Less result fetched ", len(res), len(keys))
			} else {
				t.Log("Equal result fetched ", len(res), len(keys))
			}
			for k, v := range res {
				if k.ID() == "k1" && v != "v#k1#" {
					t.Errorf("wrong value of key k1 %v", v)
				}
				if k.ID() == "k2" && v != "v###" {
					t.Errorf("wrong value of key k2 %v", v)
				}
			}
		}()
	}
	wg.Wait()
	size := 0
	count.Range(func(key, value interface{}) bool {
		size++
		return true
	})
	if size > keyCount {
		t.Errorf("Fetch called more than expected time, count: %v", size)
	} else {
		t.Logf("Fetch count: %v", size)
	}
	nextPreload := cache.GetAll()

	t.Logf("Preload content size: %v", len(nextPreload))

	newCache, _ := NewCache(GetValue, GetMultiValue, 20)
	newCache.Preload(nextPreload)

	for _, req := range requestList {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cache.MultiGet(req)
		}()
	}
	wg.Wait()
	nSize := 0
	count.Range(func(key, value interface{}) bool {
		nSize++
		return true
	})
	if nSize != size {
		t.Errorf("Preload not as per expected, request count before %v and after %v", size, nSize)
	} else {
		t.Logf("Preload as per expected, request count before %v and after %v", size, nSize)
	}
}

func BenchmarkCache(b *testing.B) {
	rand.Seed(time.Now().Unix())
	for n := 0; n < b.N; n++ {
		cache, _ := NewCache(GetValue, GetMultiValue, 500)
		count := &sync.Map{}
		cache.CommonArgs(count)
		var wg sync.WaitGroup
		keyCount := 3000
		b.Logf("Max key count: %v", keyCount)
		requests := 10
		requestList := make([][]Key, requests)
		for i := 0; i < requests; i++ {
			keys := make([]Key, 0)
			for i := 0; i < keyCount; i++ {
				if rand.Intn(100) < 100 {
					keys = append(keys, ZKey(fmt.Sprintf("k%v", i)))
				}
			}
			requestList[i] = keys
			wg.Add(1)
			go func() {
				defer wg.Done()
				res := cache.MultiGet(keys)
				if len(res) != len(keys) {
					b.Error("Less result fetched")
				}
			}()
		}
		wg.Wait()
		size := 0
		count.Range(func(key, value interface{}) bool {
			size++
			return true
		})
		if size > keyCount {
			b.Errorf("Fetch called more than expected time, count: %v", size)
		} else {
			b.Logf("Fetch count: %v", size)
		}
		cache.Close()
	}
}

type ZKey string

func (k ZKey) ID() string {
	return string(k)
}

func GetValue(key Key, args ...interface{}) Value {
	if key.ID() == "k1" || key.ID() == "k2" {
		return nil
	}
	var count *sync.Map
	if len(args) > 0 {
		count = args[0].(*sync.Map)
	}
	if count != nil {
		count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(10000)), true)
	}
	return fmt.Sprintf("v#%v", key)
}

func GetMultiValue(keys []Key, args ...interface{}) map[Key]Value {
	time.Sleep(200 * time.Millisecond)
	var count *sync.Map
	if len(args) > 0 {
		count = args[0].(*sync.Map)
	}
	res := make(map[Key]Value)
	for _, key := range keys {
		if key.ID() == "k1" || key.ID() == "k2" {
			continue
		}
		if count != nil {
			count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(100000)), true)
		}
		res[key] = fmt.Sprintf("v#%v", key)
	}
	return res
}
