package txncache

import (
	"context"
	"fmt"
	"github.com/gocollection/txn-cache/internal/pkg/lock"
	"sync"
	"time"
)

type txnCache struct {
	store          *sync.Map       // cache entries to be stored here
	lock           *sync.Map       // read-write lock per key
	fetch          Fetch           // fetch function
	multiFetch     MultiFetch      // multi fetch function or bulk fetch
	defVal         Value           // default value
	cacheDef       bool            // caching of default value
	args           []interface{}   // common args to call fetch functions
	batchSize      int             // batch size for per batch call to multiFetch function
	attemptKey     chan Key        // keys to be pushed on channel to make fetch attempt
	execMultiFetch chan bool       // force cache to fetch values for current keys in line
	closeChan      chan bool       // close the cache process and clean the remaining
	cleanup        *sync.Once      // cleanup allow closeup only once
	calls          *sync.WaitGroup // action number of cache calls in progress
	closed         bool            // if the cache has been closed
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
		attemptKey:     make(chan Key),
		defVal:         struct{}{},
		batchSize:      batchSize,
		execMultiFetch: make(chan bool),
		closeChan:      make(chan bool),
		cleanup:        &sync.Once{},
		calls:          &sync.WaitGroup{},
	}
	// starting cache setup
	cache.setup()
	return cache, nil
}

func (tc *txnCache) setup() {
	// unique keys to make attempt  for
	uniqueKeys := make([]Key, 0)

	// execute function to fetch values for given keys
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
				(rw.(*lock.RWMutex)).Unlock()
			}
		}
	}
	go func() {
		for {
			select {
			case <-tc.execMultiFetch: // force fetch execution
				{
					go execute(uniqueKeys)
					uniqueKeys = nil
				}
			case key := <-tc.attemptKey: // a new key arrives to attempt for
				{
					uniqueKeys = append(uniqueKeys, key)
					// if the key count if equal to batch size, call execute
					if tc.batchSize > 0 && tc.batchSize == len(uniqueKeys) {
						go execute(uniqueKeys)
						uniqueKeys = nil
					}
				}
			case <-tc.closeChan: // cache close signal
				{
					finalClose := make(chan bool)
					// handle calls in progress
					go func() {
						for {
							select {
							case <-tc.execMultiFetch: // post closeup no op on exec
								{
									for _, k := range uniqueKeys {
										if rw, ok := tc.lock.Load(k.ID()); ok {
											(rw.(*lock.RWMutex)).Unlock()
										}
									}
									uniqueKeys = nil
								}
							case key := <-tc.attemptKey: // batching ignored post closeup
								{
									uniqueKeys = append(uniqueKeys, key)
								}
							case <-finalClose: // returns from for loop after all the calls are done
								{
									return
								}
							}
						}
					}()
					// close call waits for all the running get calls to get completed
					// all the new unique keys are digested and no op is performed on exec
					tc.calls.Wait()
					finalClose <- true
					return
				}
			}
		}
	}()
}

func (tc *txnCache) Get(key Key) Value {
	tc.calls.Add(1)
	defer tc.calls.Done()
	if value, ok := tc.store.Load(key.ID()); ok { // cache hit
		return value
	}
	keyRWMutex, _ := tc.lock.LoadOrStore(key.ID(), lock.NewRWMutex())
	rwMutex := keyRWMutex.(*lock.RWMutex)
	locked := rwMutex.TryLock()
	if locked {
		defer rwMutex.Unlock()
		if value, ok := tc.store.Load(key.ID()); ok { // cache hit after lock acquired
			return value
		}
		value := tc.fetch(key, tc.args...)
		if value == nil {
			if tc.cacheDef {
				value = tc.defVal
			} else {
				return tc.defVal
			}
		}
		tc.store.Store(key.ID(), value)
		return value
	} else {
		rwMutex.RLock()
		defer rwMutex.RUnlock()
		if value, ok := tc.store.Load(key.ID()); ok {
			return value
		}
		return tc.defVal
	}
}

func (tc *txnCache) MultiGet(keys []Key) map[Key]Value {
	if tc.closed { // if cache is already closed
		return map[Key]Value{}
	}
	tc.calls.Add(1)
	defer tc.calls.Done()
	var data sync.Map
	var wg sync.WaitGroup
	var keyPush sync.WaitGroup
	// key will either be a hit in the cache entry or
	// will first in queue to attempt for fetch or
	// will wait for an existing fetch attempt to get completed
	for _, key := range keys {
		if value, ok := tc.store.Load(key.ID()); ok { // cache hit
			data.Store(key, value)
			continue
		}
		keyRWMutex, _ := tc.lock.LoadOrStore(key.ID(), lock.NewRWMutex())
		rwMutex := keyRWMutex.(*lock.RWMutex)
		locked := rwMutex.TryLock() // non blocking, if already locked it will wait to read the value
		if locked {
			if value, ok := tc.store.Load(key.ID()); ok { // cache hit after lock acquired
				data.Store(key, value)
				rwMutex.Unlock()
				continue
			}
		}
		wg.Add(1)
		keyPush.Add(1)
		go func(k Key, kwg *lock.RWMutex, locked bool) {
			defer wg.Done()
			if locked {
				tc.attemptKey <- k // adding key for fetch call
			}
			keyPush.Done()
			kwg.RLock() // waiting to read the set value
			defer kwg.RUnlock()
			if value, ok := tc.store.Load(k.ID()); ok {
				data.Store(k, value)
			} else {
				data.Store(k, tc.defVal) // default value is returned if no value found in cache
			}
		}(key, rwMutex, locked)
	}
	keyPush.Wait()            // waiting for all the miss keys for their appropriate efforts
	tc.execMultiFetch <- true // forcing cache to execute fetch to unblock this call asap
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
