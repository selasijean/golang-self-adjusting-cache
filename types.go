package cache

import "context"

type Value[K comparable, V any] interface {
	// TopSortOrder returns the topological sort order of the value.
	TopSortOrder() int
	Entry[K, V]
	// OnUpdate registers a callback that is called when the value is updated.
	OnUpdate(fn func(context.Context))
	// OnPurged registers a callback that is called when the value is purged from the cache.
	OnPurged(fn func(context.Context))
	// Direct dependents provides a list of keys in the cache that directly depend on the value.
	DirectDependents() []K
}

// Entry is a generic interface for an entry in the cache.
type Entry[K comparable, V any] interface {
	// Dependencies returns the dependencies of the cache entry.
	Dependencies() []K
	// Key returns the key of the cache entry.
	Key() K
	// Value returns the value of the entry.
	Value() V
	// SetMetadata sets the metadata for the entry.
	SetMetadata(data any)
	// Metadata returns the metadata for the entry.
	Metadata() any
}

type Cache[K comparable, V any] interface {
	// Clear removes all entries from the cache.
	Clear(ctx context.Context)
	// Copy creates a deep copy of the cache.
	Copy(ctx context.Context) (Cache[K, V], error)
	// Get returns the value of the given key if it is in the cache, and a boolean indicating whether the key was found.
	Get(key K) (Value[K, V], bool)
	// Keys returns all the keys in the cache.
	Keys() []K
	// Len returns the number of entries in the cache.
	Len() int
	// Purge removes the given keys and all dependent keys from the cache.
	Purge(ctx context.Context, keys ...K)
	// Put adds the given entries to the cache.
	Put(ctx context.Context, entries ...Entry[K, V]) error
	// Recompute re-evaluates the values of the given keys using the value function provided to the cache.
	Recompute(ctx context.Context, keys ...K) error
	// Values returns all the values in the cache.
	Values() []Value[K, V]
	// WithCutoffFn sets the cutoff function for the cache.
	WithCutoffFn(fn func(ctx context.Context, key K, previous, current V) (bool, error)) Cache[K, V]
	// WithParallelism sets whether the cache should use parallelism when recomputing values.
	WithParallelism(enabled bool) Cache[K, V]
	// WithWriteBackFn sets the write back function for the cache
	//
	// The write back function is called when the value of a key is updated
	// and is useful when the value of a key is computed and then stored in an external database or service.
	WithWriteBackFn(fn func(ctx context.Context, key K, value V) error) Cache[K, V]
}
