package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/alphadose/haxmap"
	"github.com/wcharczuk/go-incr"
)

type entry[K Hashable, V any] struct {
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
func NewEntry[K Hashable, V any](key K, value V, deps []K) Entry[K, V] {
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

var defaultCacheOptions = CacheOptions{
	MaxHeightOfDependencyGraph: DefaultMaxHeight * 2, // 2x to account for the fact that each cache entry consists of a mapNIncr that is a parent of a cutoffIncr
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
		c.MaxHeightOfDependencyGraph = size * 2 // 2x to account for the fact that each cache entry consists of a mapNIncr that is a parent of a cutoffIncr
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

// Types that can be used as keys for this cache must be comparable and have a unique identifier.
type Hashable interface {
	// Identifier returns a unique identifier for the key.
	Identifier() string
}

type cache[K Hashable, V any] struct {
	options           CacheOptions
	graph             *incr.Graph
	nodes             *haxmap.Map[string, *cacheNode[K, V]]
	valueFn           func(ctx context.Context, key K) (Entry[K, V], error)
	writeBackFn       func(ctx context.Context, key K, value V) error
	cutoffFn          func(ctx context.Context, key K, previous, current V) (bool, error)
	hashFn            func(str string) uintptr
	enableParallelism bool
	parallelism       int
}

func New[K Hashable, V any](valueFn func(ctx context.Context, key K) (Entry[K, V], error), opts ...CacheOption) Cache[K, V] {
	if valueFn == nil {
		panic("valueFn is not set")
	}

	options := defaultCacheOptions
	for _, opt := range opts {
		opt(&options)
	}

	return &cache[K, V]{
		nodes:             haxmap.New[string, *cacheNode[K, V]](uintptr(options.PreallocateCacheSize)),
		graph:             createIncrGraph(options),
		valueFn:           valueFn,
		enableParallelism: options.EnableParallelism,
		parallelism:       options.Parallelism,
		options:           options,
	}
}

func (c *cache[K, V]) Get(key K) (Value[K, V], bool) {
	node, ok := c.nodes.Get(key.Identifier())
	if !ok || !node.isValid() {
		return nil, false
	}

	return node, ok
}

func (c *cache[K, V]) Put(ctx context.Context, entries ...Entry[K, V]) (err error) {
	for i := 0; i < len(entries); i++ {
		id := entries[i].Key().Identifier()
		n, ok := c.nodes.Get(id)
		if !ok {
			n, err = newCacheNode(c, entries[i])
			if err != nil {
				return err
			}
			c.nodes.Set(id, n)
			err = n.observe()
			if err != nil {
				return nil
			}
		} else {
			n.setInitialValue(entries[i].Value())
			err = c.adjustDependencies(n, entries[i].Dependencies())
			if err != nil {
				return err
			}
		}
	}

	// it is possible for the recomputeFn to use Put, which leads to calling Put during stabilization. In which case, we don't need to call stabilize again.
	if c.graph.IsStabilizing() {
		return nil
	}

	err = c.graph.Stabilize(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (c *cache[K, V]) Recompute(ctx context.Context, keys ...K) error {
	for i := 0; i < len(keys); i++ {
		node, ok := c.nodes.Get(keys[i].Identifier())
		if !ok {
			return fmt.Errorf("key not found in cache: %v", keys[i])
		}
		node.invalidate()
		node.markAsStale()
	}

	if c.enableParallelism {
		return c.graph.ParallelStabilize(ctx)
	}
	return c.graph.Stabilize(ctx)
}

func (c *cache[K, V]) Keys() []K {
	keys := make([]K, 0, c.nodes.Len())
	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		keys = append(keys, node.Key())
		return true
	})
	return keys
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
	var handlersAfterPurge []func(context.Context)
	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge = append(handlersAfterPurge, node.onPurgedHandlers...)
		}
		return true
	})

	c.nodes = haxmap.New[string, *cacheNode[K, V]](uintptr(c.options.PreallocateCacheSize))
	c.graph = createIncrGraph(c.options)

	for _, fn := range handlersAfterPurge {
		fn(ctx)
	}
}

func (c *cache[K, V]) Purge(ctx context.Context, keys ...K) {
	stack := make([]K, len(keys))
	copy(stack, keys)

	var handlersAfterPurge []func(context.Context)
	defer func() {
		for _, fn := range handlersAfterPurge {
			fn(ctx)
		}
	}()

	seen := make(map[string]struct{}, len(keys))
	for len(stack) > 0 {
		key := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		id := key.Identifier()

		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

		node, ok := c.nodes.Get(id)
		if !ok {
			continue
		}

		deps := node.DirectDependents()
		for i := 0; i < len(deps); i++ {
			if _, ok := seen[deps[i].Identifier()]; !ok {
				stack = append(stack, deps[i])
			}
		}
		node.purge(ctx)
		c.nodes.Del(id)
		if len(node.onPurgedHandlers) > 0 {
			handlersAfterPurge = append(handlersAfterPurge, node.onPurgedHandlers...)
		}
	}
}

func (c *cache[K, V]) Len() int64 {
	return int64(c.nodes.Len())
}

func (c *cache[K, V]) Copy(ctx context.Context) (Cache[K, V], error) {
	nodesMap := haxmap.New[string, *cacheNode[K, V]](uintptr(c.options.PreallocateCacheSize))
	if c.hashFn != nil {
		nodesMap.SetHasher(c.hashFn)
	}

	copy := &cache[K, V]{
		nodes:             nodesMap,
		graph:             createIncrGraph(c.options),
		valueFn:           c.valueFn,
		hashFn:            c.hashFn,
		cutoffFn:          c.cutoffFn,
		writeBackFn:       c.writeBackFn,
		enableParallelism: c.enableParallelism,
		parallelism:       c.parallelism,
		options:           c.options,
	}

	nodes := make([]Value[K, V], c.nodes.Len())
	i := 0
	c.nodes.ForEach(func(_ string, node *cacheNode[K, V]) bool {
		nodes[i] = node
		i++
		return true
	})

	sortByHeight(nodes)
	for i := 0; i < len(nodes); i++ {
		err := copy.Put(ctx, nodes[i])
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

func (c *cache[K, V]) WithHashFn(fn func(str string) uintptr) Cache[K, V] {
	if fn != nil {
		c.hashFn = fn
		c.nodes.SetHasher(fn)
	}
	return c
}

func (c *cache[K, V]) MarshalJSON() ([]byte, error) {
	values := c.Values()
	sortByHeight(values)

	entries := make([]entry[K, V], len(values))
	for i := 0; i < len(values); i++ {
		entries[i] = entry[K, V]{
			K:    values[i].Key(),
			V:    values[i].Value(),
			Deps: values[i].Dependencies(),
		}
	}

	return json.Marshal(entries)
}

func (c *cache[K, V]) UnmarshalJSON(b []byte) error {
	var entries []*entry[K, V]
	err := json.Unmarshal(b, &entries)
	if err != nil {
		return err
	}

	toPut := make([]Entry[K, V], len(entries))
	for i := range entries {
		toPut[i] = entries[i]
	}
	return c.Put(context.Background(), toPut...)
}

func (c *cache[K, V]) adjustDependencies(node *cacheNode[K, V], newDeps []K) error {
	oldDeps := node.dependencies
	if len(oldDeps) == 0 && len(newDeps) == 0 {
		return nil
	}

	// Fast path: valueFn stabilize re-entry typically returns the same deps in
	// the same order. Avoid map allocation entirely in that case.
	if len(oldDeps) == len(newDeps) {
		same := true
		for i := 0; i < len(oldDeps); i++ {
			if oldDeps[i].Identifier() != newDeps[i].Identifier() {
				same = false
				break
			}
		}
		if same {
			return nil
		}
	}

	// Single map tracks membership: initialized false for every oldDep, flipped
	// true when the id is seen in newDeps (so false = removed, true = kept/added).
	state := make(map[string]bool, len(oldDeps)+len(newDeps))
	for i := 0; i < len(oldDeps); i++ {
		state[oldDeps[i].Identifier()] = false
	}

	var added []K
	for i := 0; i < len(newDeps); i++ {
		id := newDeps[i].Identifier()
		marked, exists := state[id]
		if exists {
			if !marked {
				state[id] = true
			}
			continue
		}
		state[id] = true
		added = append(added, newDeps[i])
	}

	var removed []K
	for i := 0; i < len(oldDeps); i++ {
		id := oldDeps[i].Identifier()
		if !state[id] {
			removed = append(removed, oldDeps[i])
			state[id] = true
		}
	}

	if len(added) == 0 && len(removed) == 0 {
		return nil
	}

	for i := 0; i < len(removed); i++ {
		toBeRemoved, ok := c.nodes.Get(removed[i].Identifier())
		if !ok {
			return fmt.Errorf("dependency not found in cache: %v", removed[i])
		}
		if err := node.removeDependency(toBeRemoved); err != nil {
			return err
		}
	}

	for i := 0; i < len(added); i++ {
		toBeAdded, ok := c.nodes.Get(added[i].Identifier())
		if !ok {
			return fmt.Errorf("dependency not found in cache: %v", added[i])
		}
		if err := node.addDependency(toBeAdded); err != nil {
			return err
		}
	}
	return nil
}
