package cache

import "context"

type Value[K comparable, V any] interface {
	OnUpdate(fn func(context.Context))
	Value() V
	Dependencies() []Entry[K, V]
}

type Entry[K comparable, V any] interface {
	Dependencies() []Entry[K, V]
	Key() K
	Value() V
}

type Configurable[K comparable, V any] interface {
	WithWriteBackFn(fn func(ctx context.Context, key K, value V) error) Cache[K, V]
	WithParallelism(enabled bool) Cache[K, V]
	WithCutoffFn(fn func(ctx context.Context, previous V, current V) (bool, error)) Cache[K, V]
}

type Cache[K comparable, V any] interface {
	Configurable[K, V]
	Put(ctx context.Context, entries ...Entry[K, V]) error
	Get(key K) (Value[K, V], bool)
	Recompute(ctx context.Context, keys ...K) error
}
