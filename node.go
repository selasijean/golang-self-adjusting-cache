package cache

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
)

type cacheNode[K comparable, V any] struct {
	dependencies []Entry[K, V]

	initialValue V
	useValueFn   bool

	graph *incr.Graph

	incremental incr.Incr[V]
	valueFnIncr incr.Incr[V]
	observeIncr incr.Incr[V]
	cacheKey    incr.VarIncr[K]

	onUpdateHandlers []func(context.Context)
}

func newCacheNode[K comparable, V any](c *cache[K, V], key K, value V, dependencies []Entry[K, V]) *cacheNode[K, V] {
	graph := c.graph
	n := &cacheNode[K, V]{
		graph:        graph,
		cacheKey:     incr.Var(graph, key),
		initialValue: value,
		useValueFn:   false,
		dependencies: dependencies,
	}

	result := incr.BindContext(graph, n.cacheKey, func(ctx context.Context, bs incr.Scope, _ K) (incr.Incr[V], error) {
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

		n.valueFnIncr = incr.MapNContext(graph, func(ctx context.Context, values ...V) (V, error) {
			if !n.useValueFn {
				return n.initialValue, nil
			}

			var zero V
			key := n.cacheKey.Value()
			val, err := c.valueFn(key)
			if err != nil {
				return zero, err
			}

			if c.writeBackFn != nil {
				err = c.writeBackFn(key, val)
				if err != nil {
					return zero, err
				}
			}

			return val, nil
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
	return n.cacheKey.Value()
}

func (n *cacheNode[K, V]) Value() V {
	var zero V
	if n.incremental == nil {
		return zero
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []Entry[K, V] {
	return n.dependencies
}

func (n *cacheNode[K, V]) OnUpdate(fn func(context.Context)) {
	n.onUpdateHandlers = append(n.onUpdateHandlers, fn)
}

func (n *cacheNode[K, V]) withInitialValue(value V) *cacheNode[K, V] {
	n.initialValue = value
	return n
}

func (n *cacheNode[K, V]) withDependencies(dependencies []Entry[K, V]) *cacheNode[K, V] {
	n.dependencies = dependencies
	return n
}

func (n *cacheNode[K, V]) reconstructDependencyGraph() {
	graph := n.graph
	graph.SetStale(n.cacheKey)
	n.useValueFn = false
}

func (n *cacheNode[K, V]) observe() error {
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
