package txncache

import (
	"context"
	"fmt"
	"github.com/saurav534/txn-cache/internal"
	"sync"
)

type txnCache struct {
	store          *sync.Map
	lock           *sync.Map
	fetch          Fetch
	multiFetch     MultiFetch
	defVal         Value
	batchSize      int
	multiKeyChan   chan *keyValChan
	execMultiFetch chan bool
	closeChan      chan bool
}

type keyWrap struct {
	key Key
}

type keyValChan struct {
	key    Key
	val    chan Value
	locked bool
}

// NewCache gives an instance of txnCache for given fetch & multiFetch functions
// Fetch is the function to be used for value fetch of a single key
// MultiFetch is the batched version of fetch function
// batchSize should be passed greater than 0 if MultiFetch function is expected to be called in batch
func NewCache(fetch Fetch, multiFetch MultiFetch, batchSize int) (Cache, error) {
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
	closeChan := make(chan bool)

	store := &sync.Map{}
	lock := &sync.Map{}

	cache := &txnCache{
		store:          store,
		lock:           lock,
		fetch:          fetch,
		multiFetch:     multiFetch,
		multiKeyChan:   multiKeyChan,
		defVal:         struct{}{},
		batchSize:      batchSize,
		execMultiFetch: execMultiFetch,
		closeChan:      closeChan,
	}
	cache.setup()
	return cache, nil
}

func (tc *txnCache) setup() {
	uniqueKeys := make([]Key, 0)
	keysMap := make(map[string][]*keyWrap)
	resMap := make(map[*keyWrap]chan Value)
	execute := func() {
		mf := tc.multiFetch(uniqueKeys)
		for _, k := range uniqueKeys {
			v := mf[k]
			// if fetch function do not return any value for a key
			// ideally fetch function should handle this and return a
			// suitable value of such keys
			// default values are not cached
			if v == nil {
				v = tc.defVal
			} else {
				tc.store.Store(k.ID(), v)
			}
			if kws, ok := keysMap[k.ID()]; ok {
				for _, kw := range kws {
					resMap[kw] <- v
					delete(resMap, kw)
				}
				delete(keysMap, k.ID())
			}
		}
		uniqueKeys = nil
	}
	var mutex sync.Mutex
	go func() {
		for {
			select {
			case kr := <-tc.multiKeyChan:
				{
					mutex.Lock()
					if value, ok := tc.store.Load(kr.key.ID()); ok {
						kr.val <- value
						mutex.Unlock()
						continue
					}
					kw := &keyWrap{kr.key}
					if keys, ok := keysMap[kr.key.ID()]; ok {
						keysMap[kr.key.ID()] = append(keys, kw)
					} else {
						uniqueKeys = append(uniqueKeys, kr.key)
						keysMap[kr.key.ID()] = []*keyWrap{kw}
					}
					resMap[kw] = kr.val
					if tc.batchSize > 0 && tc.batchSize == len(uniqueKeys) {
						execute()
					}
					mutex.Unlock()
				}
			case <-tc.execMultiFetch:
				{
					mutex.Lock()
					execute()
					mutex.Unlock()
				}
			case <-tc.closeChan:
				{
					close(tc.multiKeyChan)
					close(tc.execMultiFetch)
					return
				}
			}
		}
	}()
}

func (tc *txnCache) Get(key Key) Value {
	if value, ok := tc.store.Load(key.ID()); ok {
		return value
	}
	lock, _ := tc.lock.LoadOrStore(key.ID(), &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()
	if value, ok := tc.store.Load(key.ID()); ok {
		return value
	} else {
		value := tc.fetch(key)
		if value == nil {
			return tc.defVal
		}
		tc.store.Store(key.ID(), value)
		return value
	}
}

func (tc *txnCache) MultiGet(keys []Key) map[Key]Value {
	var data sync.Map
	var wg sync.WaitGroup
	var keyPush sync.WaitGroup
	for _, key := range keys {
		if value, ok := tc.store.Load(key.ID()); ok {
			data.Store(key, value)
			continue
		}
		wg.Add(1)
		lock, _ := tc.lock.LoadOrStore(key.ID(), internal.NewMutex())
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
		res[key.(Key)] = value
		return true
	})
	return res
}

func (tc *txnCache) GetAll() map[string]Value {
	res := make(map[string]Value)
	tc.store.Range(func(key, value interface{}) bool {
		res[key.(string)] = value
		return true
	})
	return res
}

func (tc *txnCache) Preload(preload map[string]Value) {
	for k, v := range preload {
		tc.store.Store(k, v)
	}
}

func (tc *txnCache) DefaultValue(value Value) {
	tc.defVal = value
}

func (tc *txnCache) CloseWithCtx(ctx context.Context) {
	go func() {
		<-ctx.Done()
		tc.closeChan <- true
	}()
}

func (tc *txnCache) Close() {
	tc.closeChan <- true
}
