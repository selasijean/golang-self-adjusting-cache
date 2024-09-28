package cache

import (
	"context"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type ObservableValue[V any] interface {
	OnUpdate(fn func(context.Context))
	Value() V
}

type KeyValue[K any, V any] interface {
	Dependencies() []KeyValue[K, V]
	Key() K
	Value() V
}

type Cache[K any, V any] interface {
	Put(ctx context.Context, kv KeyValue[K, V]) error
	Get(key K) ObservableValue[V]
}

const (
	// DefaultMaxHeight is the default maximum number of keys that can
	// be tracked in the cache's dependency graph.
	DefaultMaxHeight = 20000
)

// OptCachePreallocateNodesSize preallocates the cache key tracking map within
// the graph with a given size number of elements for items.
//
// If not provided, no size for elements will be preallocated.
func OptCachePreallocateNodesSize(size int) func(*CacheOptions) {
	return func(c *CacheOptions) {
		c.PreallocateCacheSize = size
	}
}

// OptMaxHeightOfDependencyGraph caps the longest path within the cache's dependency graph
//
// If not provided, the default height is 20000.
func OptMaxHeightOfDependencyGraph(size int) func(*CacheOptions) {
	return func(c *CacheOptions) {
		c.PreallocateCacheSize = size
	}
}

// CacheOptions are options for the cache.
type CacheOptions struct {
	MaxHeightOfDependencyGraph int
	PreallocateCacheSize       int
}

type CacheOption func(*CacheOptions)

type cache[K comparable, V any] struct {
	graph   *incr.Graph
	nodes   map[K]*cacheNode[K, V]
	valueFn func(key K) (V, error)

	shouldUseInitialValue bool

	mu sync.Mutex
}

func New[K comparable, V any](valueFn func(key K) (V, error), opts ...CacheOption) Cache[K, V] {
	options := CacheOptions{
		MaxHeightOfDependencyGraph: DefaultMaxHeight,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &cache[K, V]{
		nodes: make(map[K]*cacheNode[K, V]),
		graph: incr.New(
			incr.OptGraphClearRecomputeHeapOnError(true),
			incr.OptGraphMaxHeight(options.MaxHeightOfDependencyGraph),
			incr.OptGraphPreallocateNodesSize(options.PreallocateCacheSize),
		),
		valueFn: valueFn,
	}
}

func (c *cache[K, V]) Put(ctx context.Context, kv KeyValue[K, V]) error {
	c.mu.Lock()
	c.shouldUseInitialValue = true

	defer func() {
		c.shouldUseInitialValue = false
		c.mu.Unlock()
	}()

	node, err := c.unsafePutRecursive(ctx, kv.Key(), kv.Value(), kv.Dependencies())
	if err != nil {
		return err
	}

	err = node.observe()
	if err != nil {
		return err
	}

	return c.graph.Stabilize(ctx)
}

func (c *cache[K, V]) Get(key K) ObservableValue[V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.nodes[key]
	if !ok {
		return nil
	}

	return node
}

func (c *cache[K, V]) unsafePutRecursive(ctx context.Context, key K, value V, dependencies []KeyValue[K, V]) (*cacheNode[K, V], error) {
	for _, dependency := range dependencies {
		key, value, deps := dependency.Key(), dependency.Value(), dependency.Dependencies()
		_, err := c.unsafePutRecursive(ctx, key, value, deps)
		if err != nil {
			return nil, err
		}
	}

	node, ok := c.nodes[key]
	if ok {
		node.withDependencies(dependencies).withInitialValue(value).reconstructDependencyGraph()
	} else {
		node = newCacheNode(key, c).withDependencies(dependencies).withInitialValue(value)
		c.nodes[key] = node
	}

	return node, nil
}
