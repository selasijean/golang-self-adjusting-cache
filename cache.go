package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"

	"github.com/alphadose/haxmap"
	"github.com/wcharczuk/go-incr"
)

type entry[K comparable, V any] struct {
	K    K   `json:"key"`
	V    V   `json:"value"`
	Deps []K `json:"dependencies"`
}

func (e *entry[K, V]) Key() K {
	return e.K
}

func (e *entry[K, V]) Value() V {
	return e.V
}

func (e *entry[K, V]) Dependencies() []K {
	return e.Deps
}

// NewEntry creates a cache entry with the given key, value, and dependencies.
func NewEntry[K hashable, V any](key K, value V, deps []K) Entry[K, V] {
	return &entry[K, V]{
		K:    key,
		V:    value,
		Deps: deps,
	}
}

const (
	// DefaultMaxHeight is the default maximum length of a path in the cache's dependency graph.
	DefaultMaxHeight = 20000
)

var DefaultCacheOptions = CacheOptions{
	MaxHeightOfDependencyGraph: DefaultMaxHeight,
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

type hashable interface {
	fmt.Stringer
	comparable
}

type cache[K hashable, V any] struct {
	opts              []CacheOption
	graph             *incr.Graph
	nodes             *haxmap.Map[string, *cacheNode[K, V]]
	keys              []K
	valueFn           func(ctx context.Context, key K) (Entry[K, V], error)
	writeBackFn       func(ctx context.Context, key K, value V) error
	cutoffFn          func(ctx context.Context, key K, previous, current V) (bool, error)
	enableParallelism bool
	parallelism       int

	stabilizationMu sync.Mutex
	graphMu         sync.Mutex
}

func New[K hashable, V any](valueFn func(ctx context.Context, key K) (Entry[K, V], error), opts ...CacheOption) Cache[K, V] {
	if valueFn == nil {
		panic("valueFn is not set")
	}

	options := DefaultCacheOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &cache[K, V]{
		nodes:             haxmap.New[string, *cacheNode[K, V]](uintptr(options.PreallocateCacheSize)),
		keys:              make([]K, 0, options.PreallocateCacheSize),
		graph:             createIncrGraph(options),
		valueFn:           valueFn,
		enableParallelism: options.EnableParallelism,
		parallelism:       options.Parallelism,
		opts:              opts,
	}
}

func (c *cache[K, V]) Get(key K) (Value[K, V], bool) {
	node, ok := c.nodes.Get(key.String())
	if !ok || !node.isValid() {
		return nil, false
	}

	return node, ok
}

func (c *cache[K, V]) Put(ctx context.Context, entries ...Entry[K, V]) error {
	nodes := make([]*cacheNode[K, V], len(entries))
	for i, entry := range entries {
		key, value, deps := entry.Key(), entry.Value(), entry.Dependencies()
		n, ok := c.nodes.Get(key.String())
		if !ok {
			n = newCacheNode(c, key, entry.Value())
			c.nodes.Set(key.String(), n)
			c.keys = append(c.keys, key)
		}
		nodes[i] = n
		err := c.adjustDependencies(n, deps)
		if err != nil {
			return err
		}
		err = n.setInitialValue(value)
		if err != nil {
			return err
		}
	}

	for _, node := range nodes {
		err := node.observe()
		if err != nil {
			return err
		}

		// deadlock guard. it is possible for the recomputeFn to use Put, which leads to calling Put during stabilization.
		if c.graph.IsStabilizing() {
			return nil
		}

		err = c.stabilize(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *cache[K, V]) Recompute(ctx context.Context, keys ...K) error {
	for _, key := range keys {
		node, ok := c.nodes.Get(key.String())
		if !ok {
			return fmt.Errorf("key not found in cache: %v", key)
		}
		node.invalidate()
		node.markAsStale()
	}

	return c.stabilize(ctx)
}

func (c *cache[K, V]) Keys() []K {
	return c.keys
}

func (c *cache[K, V]) Values() []Value[K, V] {
	values := make([]Value[K, V], c.nodes.Len())
	i := 0
	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		values[i] = node
		i++
		return true
	})

	return values
}

func (c *cache[K, V]) Clear(ctx context.Context) {
	opts := c.opts
	options := DefaultCacheOptions
	for _, opt := range opts {
		opt(&options)
	}

	handlersAfterPurge := make(map[K][]func(context.Context), c.nodes.Len())
	defer func() {
		for _, fns := range handlersAfterPurge {
			for _, fn := range fns {
				fn(ctx)
			}
		}
	}()

	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		key := node.Key()
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge[key] = append(handlersAfterPurge[key], node.onPurgedHandlers...)
		}
		return true
	})

	c.nodes = haxmap.New[string, *cacheNode[K, V]](uintptr(options.PreallocateCacheSize))
	c.graph = createIncrGraph(options)
}

func (c *cache[K, V]) Purge(ctx context.Context, keys ...K) {
	stack := make([]K, len(keys))
	copy(stack, keys)

	handlersAfterPurge := make(map[K][]func(context.Context), c.nodes.Len())
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

		node, ok := c.nodes.Get(key.String())
		if !ok {
			continue
		}

		if seen[key] {
			continue
		}

		stack = append(stack, node.DirectDependents()...)

		node.unobserve(ctx)
		c.nodes.Del(key.String())
		seen[key] = true
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge[key] = append(handlersAfterPurge[key], node.onPurgedHandlers...)
		}
	}
}

func (c *cache[K, V]) Len() int {
	return int(c.nodes.Len())
}

func (c *cache[K, V]) Copy(ctx context.Context) (Cache[K, V], error) {
	options := DefaultCacheOptions
	for _, opt := range c.opts {
		opt(&options)
	}

	copy := &cache[K, V]{
		nodes:             haxmap.New[string, *cacheNode[K, V]](uintptr(options.PreallocateCacheSize)),
		graph:             createIncrGraph(options),
		valueFn:           c.valueFn,
		enableParallelism: c.enableParallelism,
		parallelism:       c.parallelism,
		opts:              c.opts,
	}

	nodes := make([]Value[K, V], c.nodes.Len())
	i := 0
	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		nodes[i] = node
		i++
		return true
	})

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
	c.writeBackFn = fn
	return c
}

// WithParallelism sets whether the cache should use parallelism when recomputing values.
func (c *cache[K, V]) WithParallelism(enabled bool) Cache[K, V] {
	c.enableParallelism = enabled
	return c
}

func (c *cache[K, V]) WithCutoffFn(fn func(ctx context.Context, key K, previous, current V) (bool, error)) Cache[K, V] {
	c.cutoffFn = fn
	return c
}

func (c *cache[K, V]) MarshalJSON() ([]byte, error) {
	values := c.Values()
	sortByHeight(values)

	items := make([]string, 0, len(values))
	for _, v := range values {
		entry := NewEntry(v.Key(), v.Value(), v.Dependencies())
		b, err := json.Marshal(entry)
		if err != nil {
			return nil, err
		}

		items = append(items, string(b))
	}

	return []byte(fmt.Sprintf("[%s]", strings.Join(items, ","))), nil
}

func (c *cache[K, V]) UnmarshalJSON(b []byte) error {
	var entries []*entry[K, V]
	err := json.Unmarshal(b, &entries)
	if err != nil {
		return err
	}

	for _, e := range entries {
		err := c.Put(context.Background(), e)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *cache[K, V]) adjustDependencies(node *cacheNode[K, V], newDeps []K) error {
	oldDeps := node.dependencies
	added := difference(newDeps, oldDeps)
	removed := difference(oldDeps, newDeps)

	for _, k := range removed {
		toBeRemoved, ok := c.nodes.Get(k.String())
		if !ok {
			return fmt.Errorf("dependency not found in cache: %v", k)
		}
		err := node.removeDependency(toBeRemoved)
		if err != nil {
			return err
		}
	}

	for _, k := range added {
		toBeAdded, ok := c.nodes.Get(k.String())
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

func (c *cache[K, V]) stabilize(ctx context.Context) error {
	c.stabilizationMu.Lock()
	defer c.stabilizationMu.Unlock()

	if c.enableParallelism {
		return c.graph.ParallelStabilize(ctx)
	}
	return c.graph.Stabilize(ctx)
}
