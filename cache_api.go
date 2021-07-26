package txncache

type Key interface {
	Id() string
}

type Value interface{}

type Cache interface {
	Get(key Key) Value
	MultiGet(keys []Key) map[Key]Value
}

type Fetch func(Key) Value

type MultiFetch func([]Key) map[Key]Value
