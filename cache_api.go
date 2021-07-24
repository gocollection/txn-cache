package txncache

type Key interface {
	Id() string
}

type Value interface{}

type Cache interface {
	Get(key Key) Value
	GetAll(keys []Key) map[Key]Value
	GetAllParallel(keys []Key) map[Key]Value
	MultiGetAll(keys []Key) map[Key]Value
	MultiGetAllBatched(keys []Key, batchSize int32) map[Key]Value
}

type Fetch func(Key) Value

type MultiFetch func([]Key) map[Key]Value
