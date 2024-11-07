package cache

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/wcharczuk/go-incr"
)

var identifierCounter uint64

func counterIdentifierProvider() (output incr.Identifier) {
	newCounter := atomic.AddUint64(&identifierCounter, 1)
	output[15] = byte(newCounter)
	output[14] = byte(newCounter >> 8)
	output[13] = byte(newCounter >> 16)
	output[12] = byte(newCounter >> 24)
	output[11] = byte(newCounter >> 32)
	output[10] = byte(newCounter >> 40)
	output[9] = byte(newCounter >> 48)
	output[8] = byte(newCounter >> 56)
	return
}

func createIncrGraph(options CacheOptions) *incr.Graph {
	incr.SetIdentifierProvider(counterIdentifierProvider)
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

func findDirectDependents[K comparable, V any](node incr.INode) []K {
	out := []K{}

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
				out = append(out, node.Key())
				continue
			}

			stack = append(stack, child)
		}
		seen[n.Node().ID().String()] = true
	}
	return out
}

func difference[K comparable](a, b []K) []K {
	differenceMap := make(map[K]bool)
	for _, item := range b {
		differenceMap[item] = true
	}

	var diff []K
	for _, item := range a {
		if !differenceMap[item] {
			diff = append(diff, item)
		}
	}

	return diff
}

func remove[K comparable](keys []K, key K) (output []K, removed bool) {
	output = make([]K, 0, len(keys))
	for _, k := range keys {
		if k != key {
			output = append(output, k)
		} else {
			removed = true
		}
	}
	return
}

func sortByHeight[K comparable, V any](nodes []Value[K, V]) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return nodes[i].TopSortOrder() < nodes[j].TopSortOrder()
	})
}
