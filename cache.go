package txncache

import (
	"context"
	"fmt"
	"github.com/saurav534/txn-cache/internal"
	"sync"
	"time"
)

type txnCache struct {
	store          *sync.Map
	lock           *sync.Map
	fetch          Fetch
	multiFetch     MultiFetch
	defVal         Value
	cacheDef       bool
	args           []interface{}
	batchSize      int
	multiKeyChan   chan Key
	execMultiFetch chan bool
	closeChan      chan bool
	cleanup        *sync.Once
	calls          *sync.WaitGroup
	closed         bool
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
		fetch = func(key Key, args ...interface{}) Value {
			return multiFetch([]Key{key}, args...)[key]
		}
	}

	// if only fetch is provided
	if multiFetch == nil {
		multiFetch = func(keys []Key, args ...interface{}) map[Key]Value {
			var data sync.Map
			var wg sync.WaitGroup
			wg.Add(len(keys))
			for _, key := range keys {
				go func(k Key) {
					defer wg.Done()
					value := fetch(k, args...)
					if value != nil {
						data.Store(k, value)
					}
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

	cache := &txnCache{
		store:          &sync.Map{},
		lock:           &sync.Map{},
		fetch:          fetch,
		multiFetch:     multiFetch,
		multiKeyChan:   make(chan Key),
		defVal:         struct{}{},
		batchSize:      batchSize,
		execMultiFetch: make(chan bool),
		closeChan:      make(chan bool),
		cleanup:        &sync.Once{},
		calls:          &sync.WaitGroup{},
	}
	cache.setup()
	return cache, nil
}

func (tc *txnCache) setup() {
	uniqueKeys := make([]Key, 0)
	execute := func(keys []Key) {
		mf := tc.multiFetch(keys, tc.args...)
		for _, k := range keys {
			v := mf[k]
			// if fetch function do not return any value for a key
			// ideally fetch function should handle this and return a
			// suitable value of such keys
			if v == nil && tc.cacheDef {
				v = tc.defVal
			}
			if v != nil {
				tc.store.Store(k.ID(), v)
			}
			if rw, ok := tc.lock.Load(k.ID()); ok {
				(rw.(*internal.RWMutex)).Unlock()
			}
		}
	}
	setup := make(chan bool)
	go func() {
		for {
			select {
			case key := <-tc.multiKeyChan:
				{
					uniqueKeys = append(uniqueKeys, key)
					if tc.batchSize > 0 && tc.batchSize == len(uniqueKeys) {
						go execute(uniqueKeys)
						uniqueKeys = nil
					}
				}
			case <-tc.execMultiFetch:
				{
					go execute(uniqueKeys)
					uniqueKeys = nil
				}
			case <-tc.closeChan:
				{
					finalClose := make(chan bool)
					go func() {
						for {
							select {
							case key := <-tc.multiKeyChan:
								{
									uniqueKeys = append(uniqueKeys, key)
								}
							case <-tc.execMultiFetch:
								{
									for _, k := range uniqueKeys {
										if rw, ok := tc.lock.Load(k.ID()); ok {
											(rw.(*internal.RWMutex)).Unlock()
										}
									}
									uniqueKeys = nil
								}
							case <-finalClose:
								{
									return
								}
							}
						}
					}()
					tc.calls.Wait()
					finalClose <- true
					return
				}
			case <-setup:
				{
					continue
				}
			}
		}
	}()
	setup <- true
	return
}

func (tc *txnCache) Get(key Key) Value {
	tc.calls.Add(1)
	defer tc.calls.Done()
	if value, ok := tc.store.Load(key.ID()); ok {
		return value
	}
	keyRWMutex, _ := tc.lock.LoadOrStore(key.ID(), internal.NewRWMutex())
	rwMutex := keyRWMutex.(*internal.RWMutex)
	rwMutex.RLock()
	if value, ok := tc.store.Load(key.ID()); ok {
		defer rwMutex.RUnlock()
		return value
	} else {
		rwMutex.RUnlock()
		rwMutex.Lock()
		defer rwMutex.Unlock()
		value := tc.fetch(key)
		if value == nil {
			if tc.cacheDef {
				value = tc.defVal
			} else {
				return tc.defVal
			}
		}
		tc.store.Store(key.ID(), value)
		return value
	}
}

func (tc *txnCache) MultiGet(keys []Key) map[Key]Value {
	if tc.closed {
		return map[Key]Value{}
	}
	tc.calls.Add(1)
	defer tc.calls.Done()
	var data sync.Map
	var wg sync.WaitGroup
	var keyPush sync.WaitGroup
	for _, key := range keys {
		if value, ok := tc.store.Load(key.ID()); ok {
			data.Store(key, value)
			continue
		}
		keyRWMutex, _ := tc.lock.LoadOrStore(key.ID(), internal.NewRWMutex())
		rwMutex := keyRWMutex.(*internal.RWMutex)
		locked := rwMutex.TryLock()
		if locked {
			if value, ok := tc.store.Load(key.ID()); ok {
				data.Store(key, value)
				rwMutex.Unlock()
				continue
			}
		}
		wg.Add(1)
		keyPush.Add(1)
		go func(k Key, kwg *internal.RWMutex, locked bool) {
			defer wg.Done()
			if locked {
				tc.multiKeyChan <- k
			}
			keyPush.Done()
			kwg.RLock()
			defer kwg.RUnlock()
			if value, ok := tc.store.Load(k.ID()); ok {
				data.Store(k, value)
			} else {
				data.Store(k, tc.defVal)
			}
		}(key, rwMutex, locked)
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

func (tc *txnCache) CommonArgs(args ...interface{}) {
	tc.args = args
}

func (tc *txnCache) DefaultValue(value Value) {
	tc.defVal = value
}

func (tc *txnCache) CacheDefault() {
	tc.cacheDef = true
}

func (tc *txnCache) CloseWithCtx(ctx context.Context) {
	go func() {
		<-ctx.Done()
		tc.Close()
	}()
}

func (tc *txnCache) CloseAfter(duration time.Duration) {
	go func() {
		<-time.Tick(duration)
		tc.Close()
	}()
}

func (tc *txnCache) Close() {
	tc.cleanup.Do(func() {
		tc.closed = true
		tc.closeChan <- true
		close(tc.closeChan)
	})
}
