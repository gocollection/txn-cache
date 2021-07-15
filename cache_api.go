package txncache

type Key interface {
	Id() string
}

type Value interface{}

type Cache interface {
	Get(key Key, fetch Fetch) Value
	GetAll(keys []Key, fetch Fetch) map[Key]Value
	GetAllParallel(keys []Key, fetch Fetch) map[Key]Value
}

type Fetch func(Key) Value