package cache

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type entry[K comparable, V any] struct {
	key      K
	value    V
	deps     []K
	metadata any
}

func (e *entry[K, V]) Key() K {
	return e.key
}

func (e *entry[K, V]) Value() V {
	return e.value
}

func (e *entry[K, V]) Dependencies() []K {
	return e.deps
}

func (e *entry[K, V]) SetMetadata(data any) {
	e.metadata = data
}

func (e *entry[K, V]) Metadata() any {
	return e.metadata
}

// NewEntry creates a cache entry with the given key, value, and dependencies.
func NewEntry[K comparable, V any](key K, value V, deps []K) Entry[K, V] {
	return &entry[K, V]{
		key:   key,
		value: value,
		deps:  deps,
	}
}

const (
	// DefaultMaxHeight is the default maximum length of a path in the cache's dependency graph.
	DefaultMaxHeight = 20000
)

var DefaultCacheOptions = CacheOptions{
	MaxHeightOfDependencyGraph: DefaultMaxHeight * 4,
}

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

// OptUseParallelism enables parallel recomputation of cache keys. numCPU sets the parallelism factor, or said another way
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
	opts              []CacheOption
	graph             *incr.Graph
	nodes             map[K]*cacheNode[K, V]
	valueFn           func(ctx context.Context, key K) (Entry[K, V], error)
	writeBackFn       func(ctx context.Context, key K, value V) error
	cutoffFn          func(ctx context.Context, key K, previous, current V) (bool, error)
	enableParallelism bool
	parallelism       int

	mu              sync.RWMutex
	stabilizationMu sync.Mutex
}

func New[K comparable, V any](valueFn func(ctx context.Context, key K) (Entry[K, V], error), opts ...CacheOption) Cache[K, V] {
	if valueFn == nil {
		panic("valueFn is not set")
	}

	options := DefaultCacheOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &cache[K, V]{
		nodes:             make(map[K]*cacheNode[K, V], options.PreallocateCacheSize),
		graph:             createIncrGraph(options),
		valueFn:           valueFn,
		enableParallelism: options.EnableParallelism,
		parallelism:       options.Parallelism,
		opts:              opts,
	}
}

func (c *cache[K, V]) Get(key K) (Value[K, V], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[key]
	if !ok || !node.isValid() {
		return nil, false
	}

	return node, ok
}

func (c *cache[K, V]) Put(ctx context.Context, entries ...Entry[K, V]) error {
	nodes := make([]*cacheNode[K, V], len(entries))

	err := withWriteLock(&c.mu, func() error {
		for i, entry := range entries {
			key, value := entry.Key(), entry.Value()
			n, ok := c.nodes[key]
			if !ok {
				n = newCacheNode(c, key, entry.Value())
				c.nodes[key] = n
			}
			nodes[i] = n
			err := c.unsafeAdjustDependencies(entry)
			if err != nil {
				return err
			}
			err = n.setInitialValue(value)
			if err != nil {
				return err
			}
			n.SetMetadata(entry.Metadata())
		}
		return nil
	})
	if err != nil {
		return err
	}

	for _, node := range nodes {
		err := node.observe()
		if err != nil {
			return err
		}

		if c.graph.IsStabilizing() {
			return nil
		}

		err = c.graph.Stabilize(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cache[K, V]) Recompute(ctx context.Context, keys ...K) error {
	c.stabilizationMu.Lock()
	defer c.stabilizationMu.Unlock()

	err := withReadLock(&c.mu, func() error {
		for _, key := range keys {
			node, ok := c.nodes[key]
			if !ok {
				return fmt.Errorf("key not found in cache: %v", key)
			}
			node.markAsStale()
		}
		return nil
	})
	if err != nil {
		return err
	}

	if c.enableParallelism {
		return c.graph.ParallelStabilize(ctx)
	}

	return c.graph.Stabilize(ctx)
}

func (c *cache[K, V]) Keys() []K {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]K, len(c.nodes))
	i := 0
	for key := range c.nodes {
		keys[i] = key
		i++
	}
	return keys
}

func (c *cache[K, V]) Values() []Value[K, V] {
	c.mu.RLock()
	defer c.mu.RUnlock()

	values := make([]Value[K, V], len(c.nodes))
	i := 0
	for _, node := range c.nodes {
		values[i] = node
		i++
	}

	return values
}

func (c *cache[K, V]) Clear(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	opts := c.opts
	options := DefaultCacheOptions
	for _, opt := range opts {
		opt(&options)
	}

	handlersAfterPurge := make(map[K][]func(context.Context), len(c.nodes))
	defer func() {
		for _, fns := range handlersAfterPurge {
			for _, fn := range fns {
				fn(ctx)
			}
		}
	}()
	for _, node := range c.nodes {
		key := node.Key()
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge[key] = append(handlersAfterPurge[key], node.onPurgedHandlers...)
		}
	}

	c.nodes = make(map[K]*cacheNode[K, V], options.PreallocateCacheSize)
	c.graph = createIncrGraph(options)
}

func (c *cache[K, V]) Purge(ctx context.Context, keys ...K) {
	c.mu.Lock()
	defer c.mu.Unlock()

	stack := make([]K, len(keys))
	copy(stack, keys)

	handlersAfterPurge := make(map[K][]func(context.Context), len(c.nodes))
	defer func() {
		for _, fns := range handlersAfterPurge {
			for _, fn := range fns {
				fn(ctx)
			}
		}
	}()

	seen := make(map[K]bool)
	for len(stack) > 0 {
		key := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		node, ok := c.nodes[key]
		if !ok {
			continue
		}

		if seen[key] {
			continue
		}

		for _, dep := range node.DirectDependents() {
			stack = append(stack, dep)
		}

		node.unobserve(ctx)
		delete(c.nodes, key)
		seen[key] = true
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge[key] = append(handlersAfterPurge[key], node.onPurgedHandlers...)
		}
	}
}

func (c *cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.nodes)
}

func (c *cache[K, V]) Copy(ctx context.Context) (Cache[K, V], error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	options := DefaultCacheOptions
	for _, opt := range c.opts {
		opt(&options)
	}

	copy := &cache[K, V]{
		nodes:             make(map[K]*cacheNode[K, V], options.PreallocateCacheSize),
		graph:             createIncrGraph(options),
		valueFn:           c.valueFn,
		enableParallelism: c.enableParallelism,
		parallelism:       c.parallelism,
		opts:              c.opts,
	}

	nodes := make([]Value[K, V], len(c.nodes))
	i := 0
	for _, node := range c.nodes {
		nodes[i] = node
		i++
	}

	sortByHeight(nodes)
	for _, node := range nodes {
		err := copy.Put(ctx, node)
		if err != nil {
			return nil, err
		}
	}

	return copy, nil
}

func (c *cache[K, V]) WithWriteBackFn(fn func(ctx context.Context, key K, value V) error) Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writeBackFn = fn
	return c
}

// WithParallelism sets whether the cache should use parallelism when recomputing values.
func (c *cache[K, V]) WithParallelism(enabled bool) Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enableParallelism = enabled
	return c
}

func (c *cache[K, V]) WithCutoffFn(fn func(ctx context.Context, key K, previous, current V) (bool, error)) Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cutoffFn = fn
	return c
}

func (c *cache[K, V]) maybeAdjustDependencies(entry Entry[K, V]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.unsafeAdjustDependencies(entry)
}

func (c *cache[K, V]) unsafeAdjustDependencies(entry Entry[K, V]) error {
	key := entry.Key()
	node, ok := c.nodes[key]
	if !ok {
		return fmt.Errorf("node not found for key: %v", key)
	}

	oldDeps := node.dependencies
	newDeps := entry.Dependencies()

	added := difference(newDeps, oldDeps)
	removed := difference(oldDeps, newDeps)

	for _, k := range removed {
		toBeRemoved, ok := c.nodes[k]
		if !ok {
			return fmt.Errorf("dependency not found in cache: %v", k)
		}
		err := node.removeDependency(toBeRemoved)
		if err != nil {
			return err
		}
	}

	for _, k := range added {
		toBeAdded, ok := c.nodes[k]
		if !ok {
			return fmt.Errorf("dependency not found in cache: %v", k)
		}
		err := node.addDependency(toBeAdded)
		if err != nil {
			return err
		}
	}

	return nil
}
