package cache

import (
	"context"
	"fmt"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type cacheNode[K comparable, V any] struct {
	dependencies []Entry[K, V]

	initialValue V
	useValueFn   bool
	valid        bool

	graph *incr.Graph

	incremental incr.Incr[V]
	valueFnIncr incr.Incr[V]
	observeIncr incr.Incr[V]
	refreshKey  incr.VarIncr[K]

	onUpdateHandlers []func(context.Context)
	mu               sync.RWMutex
}

func newCacheNode[K comparable, V any](c *cache[K, V], key K, value V, dependencies []Entry[K, V]) *cacheNode[K, V] {
	graph := c.graph
	n := &cacheNode[K, V]{
		graph:        graph,
		refreshKey:   incr.Var(graph, key),
		initialValue: value,
		useValueFn:   false,
		dependencies: dependencies,
		valid:        true,
	}

	result := incr.BindContext(graph, n.refreshKey, func(ctx context.Context, bs incr.Scope, _ K) (incr.Incr[V], error) {
		deps := n.dependencies
		incrs := make([]incr.Incr[V], 0, len(deps))

		for _, dep := range deps {
			key := dep.Key()
			node, ok := c.nodes[key]
			if !ok {
				return nil, fmt.Errorf("node of dependency not found: %v", key)
			}
			incrs = append(incrs, node.incremental)
		}

		n.valueFnIncr = incr.MapNContext(graph, func(ctx context.Context, values ...V) (result V, err error) {
			var zero V
			defer func() {
				if c.writeBackFn != nil {
					err = c.writeBackFn(ctx, key, result)
					if err != nil {
						result = zero
					}
				}
			}()

			if !n.useValueFn {
				result = n.initialValue
				return
			}

			key := n.Key()
			n.valid = false // if cache.Get(key) is called within the valueFn, n.invalid enables us to invalidate cached value for the given key
			val, err := c.valueFn(ctx, key)
			if err != nil {
				return zero, err
			}
			n.valid = true
			result = val.Value()
			return
		}, incrs...)

		n.valueFnIncr.Node().OnUpdate(func(ctx context.Context) {
			if n.useValueFn {
				return
			}

			n.useValueFn = true
		})

		return n.valueFnIncr, nil
	})

	n.incremental = incr.CutoffContext(graph, result, func(ctx context.Context, previous V, current V) (bool, error) {
		cutoffFn := c.cutoffFn
		if cutoffFn == nil {
			return false, nil
		}

		return cutoffFn(ctx, previous, current)
	})

	n.incremental.Node().OnUpdate(func(ctx context.Context) {
		for _, handler := range n.onUpdateHandlers {
			handler(ctx)
		}
	})

	n.incremental.Node().SetMetadata(n)

	return n
}

func (n *cacheNode[K, V]) Key() K {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.refreshKey.Value()
}

func (n *cacheNode[K, V]) Value() V {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var zero V
	if n.incremental == nil {
		return zero
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []Entry[K, V] {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.dependencies
}

func (n *cacheNode[K, V]) DirectDependents() []Entry[K, V] {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return findDirectDependents[K, V](n.incremental)
}

func (n *cacheNode[K, V]) OnUpdate(fn func(context.Context)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.onUpdateHandlers = append(n.onUpdateHandlers, fn)
}

func (n *cacheNode[K, V]) MarkAsInvalid() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.valid = false
}

func (n *cacheNode[K, V]) MarkAsValid() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.valid = true
}

func (n *cacheNode[K, V]) IsValid() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.valid
}

func (n *cacheNode[K, V]) withInitialValue(value V) *cacheNode[K, V] {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.initialValue = value
	return n
}

func (n *cacheNode[K, V]) withDependencies(dependencies []Entry[K, V]) *cacheNode[K, V] {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.dependencies = dependencies
	return n
}

func (n *cacheNode[K, V]) reconstructDependencyGraph() {
	n.mu.Lock()
	defer n.mu.Unlock()

	graph := n.graph
	graph.SetStale(n.refreshKey)
	n.useValueFn = false
}

func (n *cacheNode[K, V]) observe() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.incremental == nil {
		return nil
	}

	graph := n.graph
	if n.observeIncr == nil {
		o, err := incr.Observe(graph, n.incremental)
		if err != nil {
			return err
		}

		n.observeIncr = o
	}

	return nil
}

func (n *cacheNode[K, V]) height() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.incremental == nil {
		return 0
	}

	expertNode := incr.ExpertNode(n.incremental)
	return expertNode.Height()
}
