package cache

import (
	"sort"
	"sync"

	"github.com/wcharczuk/go-incr"
)

func createIncrGraph(options CacheOptions) *incr.Graph {
	return incr.New(
		incr.OptGraphClearRecomputeHeapOnError(true),
		incr.OptGraphMaxHeight(options.MaxHeightOfDependencyGraph),
		incr.OptGraphPreallocateNodesSize(options.PreallocateCacheSize),
		incr.OptGraphParallelism(options.Parallelism),
	)
}

func withWriteLock(lock *sync.RWMutex, fn func() error) error {
	lock.Lock()
	defer lock.Unlock()

	return fn()
}

func withReadLock(lock *sync.RWMutex, fn func() error) error {
	lock.RLock()
	defer lock.RUnlock()

	return fn()
}

func sortByHeight[K comparable, V any](nodes []*cacheNode[K, V]) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].height() < nodes[j].height()
	})
}

// this is used to invalidate the cache nodes so that valueFn that call back into the cache via a Get will receive no values and hence redo their computation
func withTemporaryInvalidation[K comparable, V any](nodes []*cacheNode[K, V], fn func() error) error {
	validateNodes := func() {
		for _, node := range nodes {
			node.MarkAsValid()
		}
	}
	invalidateNodes := func() {
		for _, node := range nodes {
			node.MarkAsInvalid()
		}
	}

	invalidateNodes()
	defer validateNodes()

	return fn()
}

func findDirectDependents[K comparable, V any](node incr.INode) []Entry[K, V] {
	out := []Entry[K, V]{}

	stack := []incr.INode{node}
	seen := map[string]bool{}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if _, ok := seen[n.Node().ID().String()]; ok {
			continue
		}

		expertNode := incr.ExpertNode(n)
		children := expertNode.Children()

		for _, child := range children {
			nodeMetadata := child.Node().Metadata()
			if node, ok := nodeMetadata.(*cacheNode[K, V]); ok && node != nil {
				out = append(out, node)
				continue
			}

			stack = append(stack, child)
		}
		seen[n.Node().ID().String()] = true
	}
	return out
}
