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

	tempValue  V
	useValueFn bool
	valid      bool

	graph *incr.Graph

	incremental  incr.Incr[V]
	valueFnIncr  incr.MapNIncr[V, V]
	observedIncr incr.ObserveIncr[V]

	cutoffFn func(context.Context, V, V) (bool, error)

	onUpdateHandlers []func(context.Context)
	onPurgedHandlers []func(context.Context)
	mu               sync.RWMutex
}

func newCacheNode[K comparable, V any](c *cache[K, V], key K, value V) *cacheNode[K, V] {
	graph := c.graph
	n := &cacheNode[K, V]{
		graph:        graph,
		key:          key,
		tempValue:    value,
		useValueFn:   false,
		dependencies: make([]K, 0),
		valid:        true,
	}

	n.cutoffFn = func(ctx context.Context, previous, current V) (bool, error) {
		if c.cutoffFn == nil {
			return false, nil
		}
		return c.cutoffFn(ctx, previous, current)
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

		if !n.useValueFn {
			result = n.tempValue
			return
		}

		// if cache.Get(key) is called within the valueFn, n.invalid enables us to invalidate cached value for the given key
		n.valid = false
		val, err := c.valueFn(ctx, key)
		if err != nil {
			return zero, err
		}
		n.valid = true

		result = val.Value()
		n.tempValue = result

		// reevaluating valueFn may change the dependencies of the node so we may need to update the graph
		err = c.maybeAdjustDependencies(val)
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

	n.incremental = incr.CutoffContext(graph, n.valueFnIncr, n.cutoffFn)
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

	return n.key
}

func (n *cacheNode[K, V]) Value() V {
	n.mu.RLock()
	defer n.mu.RUnlock()

	var zero V
	if n.incremental == nil {
		return zero
	}

	if n.graph.IsStabilizing() {
		return n.tempValue
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []K {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.dependencies
}

func (n *cacheNode[K, V]) DirectDependents() []K {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return findDirectDependents[K, V](n.incremental)
}

func (n *cacheNode[K, V]) Height() int {
	n.mu.RLock()
	defer n.mu.RUnlock()

	expertNode := incr.ExpertNode(n.incremental)
	return expertNode.Height()
}

func (n *cacheNode[K, V]) OnUpdate(fn func(context.Context)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.onUpdateHandlers = append(n.onUpdateHandlers, fn)
}

func (n *cacheNode[K, V]) OnPurged(fn func(context.Context)) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.onPurgedHandlers = append(n.onPurgedHandlers, fn)
}

//
// Internal methods
//

func (n *cacheNode[K, V]) observe() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.incremental == nil {
		return nil
	}

	graph := n.graph
	if n.observedIncr == nil {
		o, err := incr.Observe(graph, n.incremental)
		if err != nil {
			return err
		}

		n.observedIncr = o
	}

	return nil
}

func (n *cacheNode[K, V]) unobserve(ctx context.Context) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.observedIncr == nil {
		return
	}

	n.observedIncr.Unobserve(ctx)
}

func (n *cacheNode[K, V]) setInitialValue(value V) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.tempValue = value
	n.useValueFn = false

	n.unsafeMarkAsStale()
	return nil
}

func (n *cacheNode[K, V]) addDependency(node *cacheNode[K, V]) error {
	n.mu.Lock()
	defer n.mu.Unlock()

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
	return n.valueFnIncr.AddInput(node.incremental)
}

func (n *cacheNode[K, V]) removeDependency(node *cacheNode[K, V]) error {
	n.mu.Lock()
	defer n.mu.Unlock()

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
	return n.valueFnIncr.RemoveInput(id)
}

func (n *cacheNode[K, V]) markAsStale() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.unsafeMarkAsStale()
}

func (n *cacheNode[K, V]) unsafeMarkAsStale() {
	if !n.graph.Has(n.valueFnIncr) {
		return
	}

	n.graph.SetStale(n.valueFnIncr)
}

func (n *cacheNode[K, V]) isValid() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.valid
}
