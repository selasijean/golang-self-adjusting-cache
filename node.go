package cache

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/wcharczuk/go-incr"
)

type cacheNode[K comparable, V any] struct {
	dependencies []K
	key          K

	value      *V
	useValueFn bool

	graph *incr.Graph

	incremental  incr.Incr[V]
	valueFnIncr  incr.MapNIncr[V, V]
	observedIncr incr.ObserveIncr[V]

	onUpdateHandlers []func(context.Context)
	onPurgedHandlers []func(context.Context)

	metadata any
	graphMu  *sync.Mutex
}

func newCacheNode[K comparable, V any](c *cache[K, V], key K, value V) *cacheNode[K, V] {
	graph := c.graph
	n := &cacheNode[K, V]{
		graph:        graph,
		key:          key,
		value:        &value,
		useValueFn:   false,
		dependencies: make([]K, 0),
		graphMu:      &c.graphMu,
	}

	cutoffFn := func(ctx context.Context, previous, current V) (bool, error) {
		if c.cutoffFn == nil {
			return false, nil
		}
		return c.cutoffFn(ctx, key, previous, current)
	}

	incrs := make([]incr.Incr[V], 0, len(n.dependencies))
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

		if !n.useValueFn && n.value != nil {
			result = *n.value
			return
		}

		// if cache.Get(key) is called within the valueFn, n.hasValue enables us to invalidate cached value for the given key
		n.value = nil
		val, err := c.valueFn(ctx, key)
		if err != nil {
			return zero, err
		}

		result = val.Value()
		n.value = &result

		// reevaluating valueFn may change the dependencies of the node so we may need to update the graph
		err = c.maybeAdjustDependencies(n, val.Dependencies())
		if err != nil {
			result = zero
		}

		expertNode := incr.ExpertNode(n.valueFnIncr)
		// adjusting dependencies may add the node back to the recompute heap although we've already reevaluated valueFn so we opt out of reevaluating valueFn if that's the case
		if expertNode.HeightInRecomputeHeap() != incr.HeightUnset {
			n.useValueFn = false
		}

		return
	}, incrs...)

	n.valueFnIncr.Node().OnUpdate(func(ctx context.Context) {
		if n.useValueFn {
			return
		}
		n.useValueFn = true
	})

	n.incremental = incr.CutoffContext(graph, n.valueFnIncr, cutoffFn)
	n.incremental.Node().OnUpdate(func(ctx context.Context) {
		for _, handler := range n.onUpdateHandlers {
			handler(ctx)
		}
	})
	n.incremental.Node().SetMetadata(n)
	return n
}

func (n *cacheNode[K, V]) Key() K {
	return n.key
}

func (n *cacheNode[K, V]) Value() V {
	var zero V
	if n.incremental == nil {
		return zero
	}

	if n.graph.IsStabilizing() && n.value != nil {
		return *n.value
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []K {
	return n.dependencies
}

func (n *cacheNode[K, V]) DirectDependents() []K {
	return findDirectDependents[K, V](n.incremental)
}

func (n *cacheNode[K, V]) Metadata() any {
	return n.metadata
}

func (n *cacheNode[K, V]) TopSortOrder() int {
	expertNode := incr.ExpertNode(n.incremental)
	return expertNode.Height()
}

func (n *cacheNode[K, V]) OnUpdate(fn func(context.Context)) {
	n.onUpdateHandlers = append(n.onUpdateHandlers, fn)
}

func (n *cacheNode[K, V]) OnPurged(fn func(context.Context)) {
	n.onPurgedHandlers = append(n.onPurgedHandlers, fn)
}

//
// Internal methods
//

func (n *cacheNode[K, V]) observe() error {
	if n.incremental == nil {
		return nil
	}

	graph := n.graph
	if n.observedIncr == nil {
		n.graphMu.Lock()
		defer n.graphMu.Unlock()

		o, err := incr.Observe(graph, n.incremental)
		if err != nil {
			return err
		}

		n.observedIncr = o
	}

	return nil
}

func (n *cacheNode[K, V]) unobserve(ctx context.Context) {
	if n.observedIncr == nil {
		return
	}

	n.graphMu.Lock()
	defer n.graphMu.Unlock()

	n.observedIncr.Unobserve(ctx)
}

func (n *cacheNode[K, V]) setInitialValue(value V) error {
	n.value = &value
	n.useValueFn = false

	n.markAsStale()
	return nil
}

func (n *cacheNode[K, V]) addDependency(node *cacheNode[K, V]) error {
	if node == nil {
		return fmt.Errorf("node is nil")
	}

	if node.incremental == nil {
		return fmt.Errorf("node has no incremental: %v", node.Key())
	}

	if slices.Contains(n.dependencies, node.Key()) {
		return nil
	}

	n.dependencies = append(n.dependencies, node.Key())

	n.graphMu.Lock()
	defer n.graphMu.Unlock()

	return n.valueFnIncr.AddInput(node.incremental)
}

func (n *cacheNode[K, V]) removeDependency(node *cacheNode[K, V]) error {
	if node == nil {
		return fmt.Errorf("node is nil")
	}

	if node.incremental == nil {
		return fmt.Errorf("node has no incremental: %v", node.Key())
	}

	deps, removed := remove(n.dependencies, node.Key())
	if !removed {
		return nil
	}

	n.dependencies = deps
	id := node.incremental.Node().ID()

	n.graphMu.Lock()
	defer n.graphMu.Unlock()

	return n.valueFnIncr.RemoveInput(id)
}

func (n *cacheNode[K, V]) markAsStale() {
	n.graphMu.Lock()
	defer n.graphMu.Unlock()

	if !n.graph.Has(n.valueFnIncr) {
		return
	}

	n.graph.SetStale(n.valueFnIncr)
}

func (n *cacheNode[K, V]) isValid() bool {
	return n.value != nil
}

func (n *cacheNode[K, V]) invalidate() {
	n.value = nil
}
