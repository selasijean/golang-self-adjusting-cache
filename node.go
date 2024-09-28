package cache

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
)

type cacheNode[K comparable, V any] struct {
	dependencies []KeyValue[K, V]

	initialValue V

	cache *cache[K, V]

	incremental incr.Incr[V]
	observeIncr incr.Incr[V]
	cacheKey    incr.VarIncr[K]

	onUpdateHandlers []func(context.Context)
}

func newCacheNode[K comparable, V any](key K, c *cache[K, V]) *cacheNode[K, V] {
	graph := c.graph
	n := &cacheNode[K, V]{
		cache:    c,
		cacheKey: incr.Var(graph, key),
	}

	n.incremental = incr.BindContext(graph, n.cacheKey, func(ctx context.Context, bs incr.Scope, _ K) (incr.Incr[V], error) {
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

		return incr.MapNContext(graph, func(ctx context.Context, values ...V) (V, error) {
			if c.shouldUseInitialValue {
				return n.initialValue, nil
			}

			key := n.cacheKey.Value()
			return c.valueFn(key)
		}, incrs...), nil
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
	return n.cacheKey.Value()
}

func (n *cacheNode[K, V]) Value() V {
	var zero V
	if n.incremental == nil {
		return zero
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []KeyValue[K, V] {
	return n.dependencies
}

func (n *cacheNode[K, V]) OnUpdate(fn func(context.Context)) {
	n.onUpdateHandlers = append(n.onUpdateHandlers, fn)
}

func (n *cacheNode[K, V]) withInitialValue(value V) *cacheNode[K, V] {
	n.initialValue = value
	return n
}

func (n *cacheNode[K, V]) withDependencies(dependencies []KeyValue[K, V]) *cacheNode[K, V] {
	n.dependencies = dependencies
	return n
}

func (n *cacheNode[K, V]) reconstructDependencyGraph() {
	graph := n.cache.graph
	graph.SetStale(n.cacheKey)
}

func (n *cacheNode[K, V]) observe() error {
	if n.incremental == nil {
		return nil
	}

	graph := n.cache.graph
	if n.observeIncr == nil {
		o, err := incr.Observe(graph, n.incremental)
		if err != nil {
			return err
		}

		n.observeIncr = o
	}

	return nil
}
