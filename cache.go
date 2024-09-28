package cache

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type Value[V any] interface {
	OnUpdate(fn func(context.Context))
	Value() V
}

type Entry[K comparable, V any] interface {
	Dependencies() []Entry[K, V]
	Key() K
	Value() V
}

type Configurable[K comparable, V any] interface {
	WithWriteBackFn(fn func(key K, value V) error) Configurable[K, V]
	WithParallelism(enabled bool) Configurable[K, V]
	WithValueFn(fn func(key K) (V, error)) Configurable[K, V]
	WithCutoffFn(fn func(ctx context.Context, previous V, current V) (bool, error)) Configurable[K, V]
}

type Cache[K comparable, V any] interface {
	Configurable[K, V]
	Put(ctx context.Context, entries ...Entry[K, V]) error
	Get(key K) (Value[V], bool)
}

const (
	// DefaultMaxHeight is the default maximum number of keys that can
	// be tracked in the cache's dependency graph.
	DefaultMaxHeight = 20000
)

// OptCachePreallocateNodesSize preallocates the size of the cache
//
// If not provided, no size for elements will be preallocated.
func OptPreallocateSize(size int) func(*CacheOptions) {
	return func(c *CacheOptions) {
		c.PreallocateCacheSize = size
	}
}

// OptMaxHeightOfDependencyGraph caps the longest path within the cache's dependency graph
//
// If not provided, the default height is 20000.
func OptMaxHeightOfDependencyGraph(size int) func(*CacheOptions) {
	return func(c *CacheOptions) {
		c.MaxHeightOfDependencyGraph = size
	}
}

// OptUseParallelism enables parallel recomputation. numCPU sets the parallelism factor, or said another way
// the number of goroutines, to use
//
// numCPU will default to [runtime.NumCPU] if unset.
func OptUseParallelism(numCPU *int) func(*CacheOptions) {
	return func(c *CacheOptions) {
		c.EnableParallelism = true
		if numCPU == nil {
			c.Parallelism = runtime.NumCPU()
			return
		}

		if *numCPU < 1 {
			c.Parallelism = 1 // Ensure at least one goroutine
		} else {
			c.Parallelism = *numCPU
		}
	}
}

// CacheOptions are options for the cache.
type CacheOptions struct {
	MaxHeightOfDependencyGraph int
	PreallocateCacheSize       int
	Parallelism                int
	EnableParallelism          bool
}

type CacheOption func(*CacheOptions)

type cache[K comparable, V any] struct {
	graph             *incr.Graph
	nodes             map[K]*cacheNode[K, V]
	valueFn           func(key K) (V, error)
	writeBackFn       func(key K, value V) error
	cutoffFn          func(ctx context.Context, previous V, current V) (bool, error)
	enableParallelism bool

	mu sync.RWMutex
}

func New[K comparable, V any](valueFn func(key K) (V, error), opts ...CacheOption) Cache[K, V] {
	if valueFn == nil {
		panic("valueFn is not set")
	}

	options := CacheOptions{
		MaxHeightOfDependencyGraph: DefaultMaxHeight,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &cache[K, V]{
		nodes: make(map[K]*cacheNode[K, V], options.PreallocateCacheSize),
		graph: incr.New(
			incr.OptGraphClearRecomputeHeapOnError(true),
			incr.OptGraphMaxHeight(options.MaxHeightOfDependencyGraph),
			incr.OptGraphPreallocateNodesSize(options.PreallocateCacheSize),
			incr.OptGraphParallelism(options.Parallelism),
		),
		valueFn:           valueFn,
		enableParallelism: options.EnableParallelism,
	}
}

func (c *cache[K, V]) Put(ctx context.Context, entries ...Entry[K, V]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	visited := make(map[K]bool)
	for _, entry := range entries {
		node, err := c.unsafePut(ctx, entry.Key(), entry.Value(), entry.Dependencies(), visited)
		if err != nil {
			return err
		}

		err = node.observe()
		if err != nil {
			return err
		}
	}

	if c.enableParallelism {
		return c.graph.ParallelStabilize(ctx)
	}

	return c.graph.Stabilize(ctx)
}

func (c *cache[K, V]) Get(key K) (Value[V], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[key]
	return node, ok
}

func (c *cache[K, V]) WithWriteBackFn(fn func(key K, value V) error) Configurable[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writeBackFn = fn
	return c
}

func (c *cache[K, V]) WithParallelism(enabled bool) Configurable[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enableParallelism = enabled
	return c
}

func (c *cache[K, V]) WithValueFn(fn func(key K) (V, error)) Configurable[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fn == nil {
		panic("valueFn is not set")
	}

	c.valueFn = fn
	return c
}

func (c *cache[K, V]) WithCutoffFn(fn func(ctx context.Context, previous V, current V) (bool, error)) Configurable[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cutoffFn = fn
	return c
}

func (c *cache[K, V]) unsafePut(ctx context.Context, key K, value V, dependencies []Entry[K, V], visited map[K]bool) (*cacheNode[K, V], error) {
	if visited[key] {
		return nil, fmt.Errorf("cyclic dependency detected for key: %v", key)
	}

	visited[key] = true
	defer func() {
		delete(visited, key)
	}()

	for _, dependency := range dependencies {
		depKey := dependency.Key()
		depValue := dependency.Value()
		depDeps := dependency.Dependencies()

		_, err := c.unsafePut(ctx, depKey, depValue, depDeps, visited)
		if err != nil {
			return nil, err
		}
	}

	node, ok := c.nodes[key]
	if ok {
		node.withDependencies(dependencies).withInitialValue(value).
			reconstructDependencyGraph()
	} else {
		node = newCacheNode(c, key, value, dependencies)
		c.nodes[key] = node
	}

	return node, nil
}
