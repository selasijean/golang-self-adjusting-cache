package cache

import "context"

type Value[K comparable, V any] interface {
	Entry[K, V]
	// OnUpdate registers a callback that is called when the value is updated.
	// Useful for when you have a reference to a value in the cache
	OnUpdate(fn func(context.Context))
	// Direct dependents provides a list of entries in the cache that depend on the value
	DirectDependents() []Entry[K, V]
}

// Entry is a generic interface for an entry in the cache
type Entry[K comparable, V any] interface {
	// Dependencies returns the dependencies of the cache entry
	Dependencies() []Entry[K, V]
	// Key returns the key of the cache entry
	Key() K
	// Value returns the value of the entry
	Value() V
}

// Cache is a generic interface for an online cache that can be used to store and retrieve values and that can automatically
// recompute cache entries when their dependencies in the cache are updated.
type Cache[K comparable, V any] interface {
	// AutoPut adds the given keys to the cache and computes their values if they are not already in the cache
	AutoPut(ctx context.Context, keys ...K) error
	// Put adds the given entries to the cache
	Put(ctx context.Context, entries ...Entry[K, V]) error
	// Get returns the value of the given key if it is in the cache, and a boolean indicating whether the key was found
	Get(key K) (Value[K, V], bool)
	// Recompute recomputes the values of the given keys in the cache. The difference between this and AutoPut is that
	// It is different from AutoPut in that it throws an error if any of the keys provided are not already in the cache
	Recompute(ctx context.Context, keys ...K) error
	// Len returns the number of entries in the cache
	Len() int
	// Clear removes all entries from the cache
	Clear()
	// Purge removes the given keys from the cache, and any keys that depend on them
	Purge(ctx context.Context, keys ...K)
	// WithWriteBackFn sets the write back function for the cache
	WithWriteBackFn(fn func(ctx context.Context, key K, value V) error) Cache[K, V]
	// WithParallelism sets whether the cache should use parallelism when recomputing values
	WithParallelism(enabled bool) Cache[K, V]
	// WithCutoffFn sets the cutoff function for the cache
	WithCutoffFn(fn func(ctx context.Context, previous V, current V) (bool, error)) Cache[K, V]
}
