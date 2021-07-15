package txncache

import "sync"

type TxnCache struct {
	store map[Key]Value
	lock  sync.Map
}

func (tc *TxnCache) Get(key Key, fetch Fetch) Value {
	if value, ok := tc.store[key]; ok {
		return value
	}
	lock, _ := tc.lock.LoadOrStore(key, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	if value, ok := tc.store[key]; ok {
		return value
	} else {
		value := fetch(key)
		tc.store[key] = value
		return value
	}
}

func (tc *TxnCache) GetAll(keys []Key, fetch Fetch) map[Key]Value {
	res := make(map[Key]Value)
	for _, key := range keys {
		res[key] = tc.Get(key, fetch)
	}
	return res
}

func (tc *TxnCache) GetAllParallel(keys []Key, fetch Fetch) map[Key]Value {
	var data sync.Map
	var wg sync.WaitGroup
	for _, key := range keys {
		go func(k Key) {
			wg.Add(1)
			defer wg.Done()
			data.Store(k, tc.Get(k, fetch))
		}(key)
	}
	res := make(map[Key]Value)
	wg.Wait()
	data.Range(func(key, value interface{}) bool {
		res[key.(Key)] = value.(Value)
		return true
	})
	return res
}
