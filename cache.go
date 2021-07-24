package txncache

import (
	"context"
	"fmt"
	"sync"
)

type TxnCache struct {
	store          *sync.Map
	lock           *sync.Map
	fetch          Fetch
	multiFetch     MultiFetch
	multiKeyChan   chan *keyValChan
	execMultiFetch chan bool
}

type keyWrap struct {
	key Key
}

type keyValChan struct {
	key Key
	val chan Value
}

func NewTxnCache(ctx context.Context, fetch Fetch, multiFetch MultiFetch) (*TxnCache, error) {
	if fetch == nil && multiFetch == nil {
		return nil, fmt.Errorf("both fetch and multifetch can not be nil")
	}

	// if only multiFetch is provided
	if fetch == nil {
		fetch = func(key Key) Value {
			return multiFetch([]Key{key})[key]
		}
	}

	// if only fetch is provided
	if multiFetch == nil {
		multiFetch = func(keys []Key) map[Key]Value {
			var data sync.Map
			for _, key := range keys {
				go func(k Key) {
					data.Store(k, fetch(k))
				}(key)
			}
			res := make(map[Key]Value)
			data.Range(func(key, value interface{}) bool {
				res[key.(Key)] = value.(Value)
				return true
			})
			return res
		}
	}

	multiKeyChan := make(chan *keyValChan)
	execMultiFetch := make(chan bool)

	store := &sync.Map{}
	lock := &sync.Map{}

	uniqueKeys := make([]Key, 0)
	keysMap := make(map[string][]*keyWrap)
	resMap := make(map[*keyWrap]chan Value)
	var mutex sync.Mutex
	go func() {
		for {
			select {
			case kr := <-multiKeyChan:
				{
					mutex.Lock()
					kw := &keyWrap{kr.key}
					if keys, ok := keysMap[kr.key.Id()]; ok {
						keysMap[kr.key.Id()] = append(keys, kw)
					} else {
						uniqueKeys = append(uniqueKeys, kr.key)
						keysMap[kr.key.Id()] = []*keyWrap{kw}
					}
					resMap[kw] = kr.val
					mutex.Unlock()
				}
			case <-execMultiFetch:
				{
					mutex.Lock()
					for k, v := range multiFetch(uniqueKeys) {
						store.Store(k.Id(), v)
						if kws, ok := keysMap[k.Id()]; ok {
							for _, kw := range kws {
								resMap[kw] <- v
								delete(resMap, kw)
							}
							delete(keysMap, k.Id())
						}
					}
					uniqueKeys = nil
					mutex.Unlock()
				}
			case <-ctx.Done():
				{
					close(multiKeyChan)
					close(execMultiFetch)
					return
				}
			}
		}
	}()
	return &TxnCache{
		store:          store,
		lock:           lock,
		fetch:          fetch,
		multiFetch:     multiFetch,
		multiKeyChan:   multiKeyChan,
		execMultiFetch: execMultiFetch,
	}, nil
}

func (tc *TxnCache) Get(key Key) Value {
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
		value := tc.fetch(key)
		tc.store.Store(key.Id(), value)
		return value
	}
}

func (tc *TxnCache) GetAll(keys []Key) map[Key]Value {
	res := make(map[Key]Value)
	for _, key := range keys {
		res[key] = tc.Get(key)
	}
	return res
}

func (tc *TxnCache) GetAllParallel(keys []Key) map[Key]Value {
	var data sync.Map
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func(k Key) {
			defer wg.Done()
			data.Store(k, tc.Get(k))
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

func (tc *TxnCache) MultiGetAll(keys []Key) map[Key]Value {
	var data sync.Map
	var wg sync.WaitGroup
	var keyPush sync.WaitGroup
	for _, key := range keys {
		//if value, ok := tc.store.Load(key.Id()); ok {
		//	data.Store(key, value)
		//	continue
		//}
		wg.Add(1)
		lock, _ := tc.lock.LoadOrStore(key.Id(), &sync.Mutex{})
		mutex := lock.(*sync.Mutex)
		mutex.Lock()
		keyPush.Add(1)
		go func(k Key) {
			defer wg.Done()
			defer mutex.Unlock()
			if value, ok := tc.store.Load(k.Id()); ok {
				data.Store(k, value)
				keyPush.Done()
				return
			}
			valChan := make(chan Value)
			tc.multiKeyChan <- &keyValChan{k, valChan}
			keyPush.Done()
			val := <-valChan
			data.Store(k, val)
		}(key)
	}

	keyPush.Wait()
	tc.execMultiFetch <- true
	res := make(map[Key]Value)
	wg.Wait()
	data.Range(func(key, value interface{}) bool {
		res[key.(Key)] = value.(Value)
		return true
	})
	return res
}

func (tc *TxnCache) MultiGetAllBatched(keys []Key, batchSize int32) map[Key]Value {
	panic("implement me")
}
