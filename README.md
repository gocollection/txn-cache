# Go Transaction Cache

**txncache** pkg helps reuse Fetch value of multiple keys during a single request handling.

## Use Case

during a request handling one might need to get value for a given key from a remote resource e.g redis, api, database etc. All such keys might not be available in the starting of the request so one might end up calling the Fetch function multiple times for the same key if keys are repeated. Provided one might process a request in parallel, it's possible to even fetch value of the same key not just multiple times but also at the same time as well, hence simple caching won't be enough here.

txncache is the abstraction to make sure we do not fetch the value of the same key more than once and at the same time use the efficient caching layer for handling multiple such keys with repeated occurrence among multiple goroutines.

## Usage

Key should implement the interface -
```go
type Key interface {
	Id() string
}
``` 
this helps identify repeated key basic it returned id

Value can be anything -
```go
type Value interface{}
```

Fetch function should follow the signature -
```go
type Fetch func(Key) Value
```

That's all

```go
func main() {
	cache := txncache.NewTxnCache()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		keys := make([]txncache.Key, 0)
		keys = append(keys, ZKey("k1"))
		keys = append(keys, ZKey("k2"))
		keys = append(keys, ZKey("k1"))

		cache.GetAllParallel(keys, GetValue)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		keys := make([]txncache.Key, 0)
		keys = append(keys, ZKey("k2"))
		keys = append(keys, ZKey("k3"))
		keys = append(keys, ZKey("k2"))

		cache.GetAllParallel(keys, GetValue)
	}()
	wg.Wait()
	// k1, k2, k3 will be fetched only once
}

func GetValue(key txncache.Key) txncache.Value {
	return fmt.Sprintf("val#%v", key)
}

type ZKey string

func (k ZKey) Id() string {
	return string(k)
}

```