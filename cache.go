package cache

import (
	"context"
	"fmt"
	"runtime"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type entry[K comparable, V any] struct {
	key   K
	value V
	deps  []K
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
		c.MaxHeightOfDependencyGraph = size * 2
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

// Cache is a generic interface for an online cache that can be used to store and retrieve values and that can automatically
// recompute cache entries when their dependencies in the cache are updated.
type Cache[K comparable, V any] struct {
	opts              []CacheOption
	graph             *incr.Graph
	nodes             map[K]*cacheNode[K, V]
	valueFn           func(ctx context.Context, key K) (Entry[K, V], error)
	writeBackFn       func(ctx context.Context, key K, value V) error
	cutoffFn          func(ctx context.Context, previous V, current V) (bool, error)
	enableParallelism bool
	parallelism       int

	mu              sync.RWMutex
	stabilizationMu sync.Mutex
}

// New creates a new cache with the given value function and options.
func New[K comparable, V any](valueFn func(ctx context.Context, key K) (Entry[K, V], error), opts ...CacheOption) *Cache[K, V] {
	if valueFn == nil {
		panic("valueFn is not set")
	}

	options := CacheOptions{
		MaxHeightOfDependencyGraph: DefaultMaxHeight * 4,
	}
	for _, opt := range opts {
		opt(&options)
	}

	return &Cache[K, V]{
		nodes:             make(map[K]*cacheNode[K, V], options.PreallocateCacheSize),
		graph:             createIncrGraph(options),
		valueFn:           valueFn,
		enableParallelism: options.EnableParallelism,
		parallelism:       options.Parallelism,
		opts:              opts,
	}
}

// Get returns the value of the given key if it is in the cache, and a boolean indicating whether the key was found.
func (c *Cache[K, V]) Get(key K) (Value[K, V], bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[key]
	if !ok || !node.isValid() {
		return nil, false
	}

	return node, ok
}

// Put adds the given entries to the cache.
func (c *Cache[K, V]) Put(ctx context.Context, entries ...Entry[K, V]) error {
	nodes := make([]*cacheNode[K, V], 0, len(entries))

	err := withWriteLock(&c.mu, func() error {
		for _, entry := range entries {
			key, value := entry.Key(), entry.Value()
			n, ok := c.nodes[key]
			if !ok {
				n = newCacheNode(c, key, entry.Value())
				c.nodes[key] = n
			}
			nodes = append(nodes, n)
			err := c.unsafeAdjustDependencies(entry)
			if err != nil {
				return err
			}
			err = n.setInitialValue(value)
			if err != nil {
				return err
			}
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

// Recompute re-evaluates the values of the given keys using the value function provided to the cache.
func (c *Cache[K, V]) Recompute(ctx context.Context, keys ...K) error {
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

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()

	opts := c.opts
	options := CacheOptions{
		MaxHeightOfDependencyGraph: DefaultMaxHeight,
	}
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

// Purge removes the given keys from the cache, and keys that depend on them.
func (c *Cache[K, V]) Purge(ctx context.Context, keys ...K) {
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

// Len returns the number of entries in the cache.
func (c *Cache[K, V]) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.nodes)
}

// WithWriteBackFn sets the write back function for the cache
//
// The write back function is called when the value of a key is updated
// and is useful when the value of a key is computed and then stored in an external database or service.
func (c *Cache[K, V]) WithWriteBackFn(fn func(ctx context.Context, key K, value V) error) *Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.writeBackFn = fn
	return c
}

// WithParallelism sets whether the cache should use parallelism when recomputing values.
func (c *Cache[K, V]) WithParallelism(enabled bool) *Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.enableParallelism = enabled
	return c
}

// WithCutoffFn sets the cutoff function for the cache.
func (c *Cache[K, V]) WithCutoffFn(fn func(ctx context.Context, previous V, current V) (bool, error)) *Cache[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cutoffFn = fn
	return c
}

func (c *Cache[K, V]) maybeAdjustDependencies(entry Entry[K, V]) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.unsafeAdjustDependencies(entry)
}

func (c *Cache[K, V]) unsafeAdjustDependencies(entry Entry[K, V]) error {
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
