package cache

import (
	"context"
	"fmt"
	"slices"

	"github.com/wcharczuk/go-incr"
)

type cacheNode[K Hashable, V any] struct {
	dependencies []K
	key          K

	value      V
	valid      bool
	useValueFn bool

	graph *incr.Graph

	incremental  incr.Incr[V]
	valueFnIncr  incr.MapNIncr[V, V]
	observedIncr incr.ObserveIncr[V]

	onUpdateHandlers []func(context.Context)
	onPurgedHandlers []func(context.Context)
}

func newCacheNode[K Hashable, V any](c *cache[K, V], entry Entry[K, V]) (*cacheNode[K, V], error) {
	graph := c.graph
	n := &cacheNode[K, V]{
		graph:        graph,
		key:          entry.Key(),
		value:        entry.Value(),
		valid:        true,
		useValueFn:   false,
		dependencies: entry.Dependencies(),
	}

	incrs := make([]incr.Incr[V], len(n.dependencies))
	for i := 0; i < len(n.dependencies); i++ {
		node, ok := c.nodes.Get(n.dependencies[i].Identifier())
		if !ok {
			return nil, fmt.Errorf("dependency not found: %v", n.dependencies[i])
		}
		incrs[i] = node.incremental
	}

	n.valueFnIncr = incr.MapNContext(graph, func(ctx context.Context, values ...V) (V, error) {
		var zero V
		var result V
		var err error

		if !n.useValueFn && n.valid {
			result = n.value
		} else {
			// if cache.Get(key) is called within the valueFn, invalidating enables us to bypass the cached value for the given key
			n.valid = false
			val, valErr := c.valueFn(ctx, n.key)
			if valErr != nil {
				err = valErr
			} else {
				result = val.Value()
				n.value = result
				n.valid = true

				// reevaluating valueFn may change the dependencies of the node so we may need to update the graph
				if adjErr := c.adjustDependencies(n, val.Dependencies()); adjErr != nil {
					err = adjErr
					result = zero
				} else {
					// adjusting dependencies may add the node back to the recompute heap although we've already reevaluated valueFn so we opt out of reevaluating valueFn if that's the case
					expertNode := incr.ExpertNode(n.valueFnIncr)
					if expertNode.HeightInRecomputeHeap() != incr.HeightUnset {
						n.useValueFn = false
					}
				}
			}
		}

		if c.writeBackFn != nil {
			err = c.writeBackFn(ctx, n.key, result)
			if err != nil {
				result = zero
			}
		}

		return result, err
	}, incrs...)

	n.valueFnIncr.Node().OnUpdate(func(ctx context.Context) {
		if n.useValueFn {
			return
		}
		n.useValueFn = true
	})

	cutoffFn := func(ctx context.Context, previous, current V) (bool, error) {
		if c.cutoffFn == nil {
			return false, nil
		}
		return c.cutoffFn(ctx, n.key, previous, current)
	}

	n.incremental = incr.CutoffContext(graph, n.valueFnIncr, cutoffFn)
	n.incremental.Node().OnUpdate(func(ctx context.Context) {
		for _, handler := range n.onUpdateHandlers {
			handler(ctx)
		}
	})
	n.incremental.Node().SetMetadata(n)
	return n, nil
}

func (n *cacheNode[K, V]) Key() K {
	return n.key
}

func (n *cacheNode[K, V]) Value() V {
	var zero V
	if n.incremental == nil || n.graph == nil {
		return zero
	}

	if n.graph.IsStabilizing() && n.valid {
		return n.value
	}

	return n.incremental.Value()
}

func (n *cacheNode[K, V]) Dependencies() []K {
	return n.dependencies
}

func (n *cacheNode[K, V]) DirectDependents() []K {
	return findDirectDependents[K, V](n.incremental)
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
	if n.incremental == nil || n.graph == nil {
		return nil
	}

	if n.observedIncr == nil {
		o, err := incr.Observe(n.graph, n.incremental)
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

	n.observedIncr.Unobserve(ctx)
	n.observedIncr = nil
}

func (n *cacheNode[K, V]) setInitialValue(value V) {
	n.value = value
	n.valid = true
	n.useValueFn = false

	if n.graph == nil || n.graph.IsStabilizing() {
		return
	}

	n.markAsStale()
}

func (n *cacheNode[K, V]) addDependency(node *cacheNode[K, V]) error {
	if node == nil {
		return fmt.Errorf("node is nil")
	}

	if node.incremental == nil {
		return fmt.Errorf("node has no incremental: %v", node.Key())
	}

	n.dependencies = append(n.dependencies, node.Key())
	return n.valueFnIncr.AddInput(node.incremental)
}

func (n *cacheNode[K, V]) removeDependency(node *cacheNode[K, V]) error {
	if node == nil {
		return fmt.Errorf("node is nil")
	}

	if node.incremental == nil {
		return fmt.Errorf("node has no incremental: %v", node.Key())
	}

	targetID := node.Key().Identifier()
	idx := slices.IndexFunc(n.dependencies, func(k K) bool {
		return k.Identifier() == targetID
	})
	if idx < 0 {
		return nil
	}
	n.dependencies = slices.Delete(n.dependencies, idx, idx+1)

	id := node.incremental.Node().ID()
	return n.valueFnIncr.RemoveInput(id)
}

func (n *cacheNode[K, V]) markAsStale() {
	if n.graph == nil || n.observedIncr == nil {
		return
	}

	n.graph.SetStale(n.valueFnIncr)
}

func (n *cacheNode[K, V]) isValid() bool {
	return n.valid
}

func (n *cacheNode[K, V]) invalidate() {
	n.valid = false
}

func (n *cacheNode[K, V]) purge(ctx context.Context) {
	n.unobserve(ctx)
	n.incremental = nil
	n.observedIncr = nil
	n.graph = nil
}
