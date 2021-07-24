package txncache

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var count sync.Map

func TestCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache, _ := NewTxnCache(ctx, GetValue, GetMultiValue)
	var wg sync.WaitGroup
	rand.Seed(time.Now().Unix())
	keyCount := rand.Intn(1000)
	t.Logf("Max key count: %v", keyCount)
	for i := 0; i < rand.Intn(100); i++ {
		keys := make([]Key, 0)
		for i := 0; i < keyCount; i++ {
			if rand.Intn(100) < 50 {
				keys = append(keys, ZKey(fmt.Sprintf("k%v", i)))
			}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := cache.MultiGetAll(keys)
			for k, v := range res {
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
}

type ZKey string

func (k ZKey) Id() string {
	return string(k)
}

func GetValue(key Key) Value {
	count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(10000)), true)
	fmt.Printf("%v key fetch\n", key)
	return fmt.Sprintf("v#%v", key)
}

func GetMultiValue(keys []Key) map[Key]Value {
	res := make(map[Key]Value)
	for _, key := range keys {
		fmt.Printf("%v key fetch\n", key)
		count.Store(fmt.Sprintf("%v-%v", key, rand.Intn(10000)), true)
		res[key] = fmt.Sprintf("v#%v", key)
	}
	return res
}
