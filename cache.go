package txncache

import (
	"context"
	"fmt"
	"github.com/saurav534/txn-cache/internal"
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
	key    Key
	val    chan Value
	locked bool
}

// NewCache gives an instance of TxnCache for given fetch & multiFetch functions
// Fetch is the function to be used for value fetch of a single key
// MultiFetch is the batched version of fetch function
// batchSize should be passed greater than 0 if MultiFetch function is expected to be called in batch
func NewCache(ctx context.Context, fetch Fetch, multiFetch MultiFetch, batchSize int) (*TxnCache, error) {
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
			var wg sync.WaitGroup
			wg.Add(len(keys))
			for _, key := range keys {
				go func(k Key) {
					defer wg.Done()
					data.Store(k, fetch(k))
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
	}

	multiKeyChan := make(chan *keyValChan)
	execMultiFetch := make(chan bool)

	store := &sync.Map{}
	lock := &sync.Map{}

	uniqueKeys := make([]Key, 0)
	keysMap := make(map[string][]*keyWrap)
	resMap := make(map[*keyWrap]chan Value)
	execute := func() {
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
	}
	var mutex sync.Mutex
	go func() {
		for {
			select {
			case kr := <-multiKeyChan:
				{
					mutex.Lock()
					if value, ok := store.Load(kr.key.Id()); ok {
						kr.val <- value
						mutex.Unlock()
						continue
					}
					kw := &keyWrap{kr.key}
					if keys, ok := keysMap[kr.key.Id()]; ok {
						keysMap[kr.key.Id()] = append(keys, kw)
					} else {
						uniqueKeys = append(uniqueKeys, kr.key)
						keysMap[kr.key.Id()] = []*keyWrap{kw}
					}
					resMap[kw] = kr.val
					if batchSize > 0 && batchSize == len(uniqueKeys) {
						execute()
					}
					mutex.Unlock()
				}
			case <-execMultiFetch:
				{
					mutex.Lock()
					execute()
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

func (tc *TxnCache) MultiGet(keys []Key) map[Key]Value {
	var data sync.Map
	var wg sync.WaitGroup
	var keyPush sync.WaitGroup
	for _, key := range keys {
		if value, ok := tc.store.Load(key.Id()); ok {
			data.Store(key, value)
			continue
		}
		wg.Add(1)
		lock, _ := tc.lock.LoadOrStore(key.Id(), internal.NewMutex())
		mutex := lock.(*internal.Mutex)
		locked := mutex.TryLock()
		keyPush.Add(1)
		go func(k Key) {
			defer wg.Done()
			if locked {
				defer mutex.Unlock()
			}
			valChan := make(chan Value)
			tc.multiKeyChan <- &keyValChan{k, valChan, locked}
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
