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

Either a *Fetch* or *MultiFetch* function or both should be provided.
Fetch function should follow the signature, this function returns value for a single input key -
```go
type Fetch func(Key) Value
```

It's good to have a *MultiFetch* function which should have following signature. If *MultiFetch* is not provided, Fetch 
will be used to create a MultiFetch function on the go.
```go
type MultiFetch func([]Key) map[Key]Value
```

Following example showcase the use case where multiple goroutines tries to fetch values of multiple keys at once,
the keys are overlapping as well.

```go
func main() {
	cache, _ := txncache.NewCache(context.Background(), GetValue, GetMultiValue, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		keys := make([]txncache.Key, 0)
		keys = append(keys, ZKey("k1"))
		keys = append(keys, ZKey("k2"))
		keys = append(keys, ZKey("k1"))

		cache.MultiGet(keys)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		keys := make([]txncache.Key, 0)
		keys = append(keys, ZKey("k2"))
		keys = append(keys, ZKey("k3"))
		keys = append(keys, ZKey("k2"))

		cache.MultiGet(keys)
	}()
	wg.Wait()
	// k1, k2, k3 will be fetched only once
}

func GetValue(key txncache.Key) txncache.Value {
	fmt.Println("key fetch ", key)
	return fmt.Sprintf("val#%v", key)
}

func GetMultiValue(keys []txncache.Key) map[txncache.Key]txncache.Value {
	res := make(map[txncache.Key]txncache.Value)
	for _, key := range keys {
		fmt.Println("key fetch ", key)
		res[key] = fmt.Sprintf("v#%v", key)
	}
	return res
}

type ZKey string

func (k ZKey) Id() string {
	return string(k)
}
```

For robust example checkout the test file.