package txncache

import (
	"context"
	"time"
)

// Key construct of the cache
// ID() is used to uniquely identify Keys
type Key interface {
	ID() string
}

// Value construct of the cache
// Can be of any type
type Value interface{}

type Cache interface {
	// Get return value for a given key
	Get(Key) Value

	// MultiGet returns key value map for a given list of keys
	MultiGet([]Key) map[Key]Value

	// GetAll returns map of all the key id, value present in the cache
	GetAll() map[string]Value

	// Preload can be used to load cache with a map of key value upfront
	Preload(map[string]Value)

	// CommonArgs set arguments which are common for all the calls to the fetch functions
	CommonArgs(...interface{})

	// DefaultValue set the default value for the missing values while calling fetch functions
	// This is mandatory if the provided MultiFetch function does not guarantee value for all
	// the provided keys
	DefaultValue(Value)

	// CacheDefault enables caching default value if Fetch function does not return value
	// by default caching if disabled
	CacheDefault()

	// CloseWithCtx clean up the cache once the provided context is canceled or done
	// it's a non blocking function and can be used in place of explicit Close() call for guaranteed
	// cleanup as soon as ctx is canceled
	CloseWithCtx(context.Context)

	// CloseAfter closes the cache and clean resources after specified duration if not closed otherwise
	CloseAfter(time.Duration)

	// Close is necessary to call to cleanup cache and make it eligible for GC
	Close()
}

type Fetch func(Key, ...interface{}) Value

type MultiFetch func([]Key, ...interface{}) map[Key]Value
