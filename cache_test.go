package txncache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var count sync.Map

func TestCache(t *testing.T) {
	preload := map[string]Value{
		"k1": "v#k1#",
	}
	cache, _ := NewCache(GetValue, GetMultiValue, 10)
	cache.Preload(preload)
	cache.DefaultValue("v###")
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
				t.Error("Less result fetched")
			}
			for k, v := range res {
				if k.ID() == "k1" && v != "v#k1#" {
					t.Error("wrong value of key k1")
				}
				if k.ID() == "k2" && v != "v###" {
					t.Error("wrong value of key k2")
				}
				fmt.Printf("%v-%v\n", k, v)
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

type ZKey string

func (k ZKey) ID() string {
	return string(k)
}

func GetValue(key Key) Value {
	count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(10000)), true)
	fmt.Printf("%v key fetch\n", key)
	return fmt.Sprintf("v#%v", key)
}

func GetMultiValue(keys []Key) map[Key]Value {
	id := rand.Intn(1000)
	res := make(map[Key]Value)
	for _, key := range keys {
		if key.ID() == "k1" || key.ID() == "k2" {
			continue
		}
		fmt.Printf("%v - %v key fetch\n", id, key)
		count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(10000)), true)
		res[key] = fmt.Sprintf("v#%v", key)
	}
	return res
}
