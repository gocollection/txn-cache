package txncache

import "sync"

type TxnCache struct {
	store sync.Map
	lock  sync.Map
}

func NewTxnCache() *TxnCache {
	return &TxnCache{}
}

func (tc *TxnCache) Get(key Key, fetch Fetch) Value {
	if value, ok := tc.store.Load(key.Id()); ok {
		return value
	}
	lock, _ := tc.lock.LoadOrStore(key.Id(), &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	if value, ok := tc.store.Load(key.Id()); ok {
		return value
	} else {
		value := fetch(key)
		tc.store.Store(key.Id(), value)
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
		wg.Add(1)
		go func(k Key) {
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
