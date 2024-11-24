package cache

import (
	"sort"
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

func findDirectDependents[K Hashable, V any](node incr.INode) []K {
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

		for i := 0; i < len(children); i++ {
			nodeMetadata := children[i].Node().Metadata()
			if node, ok := nodeMetadata.(*cacheNode[K, V]); ok && node != nil {
				out = append(out, node.Key())
				continue
			}

			stack = append(stack, children[i])
		}
		seen[n.Node().ID().String()] = true
	}
	return out
}

// difference returns the elements in a that are not in b.
func difference[K Hashable](a, b []K) []K {
	if len(a) == 0 {
		return []K{}
	}

	if len(b) == 0 {
		return a
	}

	bSet := make(map[string]struct{}, len(b))
	for i := 0; i < len(b); i++ {
		bSet[b[i].Identifier()] = struct{}{}
	}

	var diff []K
	for i := 0; i < len(a); i++ {
		if _, found := bSet[a[i].Identifier()]; !found {
			diff = append(diff, a[i])
		}
	}

	return diff
}

func remove[K Hashable](keys []K, key K) (output []K, removed bool) {
	output = make([]K, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		if keys[i].Identifier() != key.Identifier() {
			output = append(output, keys[i])
		} else {
			removed = true
		}
	}
	return
}

func contains[K Hashable](keys []K, key K) bool {
	for i := 0; i < len(keys); i++ {
		if keys[i].Identifier() == key.Identifier() {
			return true
		}
	}
	return false
}

func sortByHeight[K Hashable, V any](nodes []Value[K, V]) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return nodes[i].TopSortOrder() < nodes[j].TopSortOrder()
	})
}
