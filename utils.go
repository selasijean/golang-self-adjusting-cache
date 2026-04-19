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
	var out []K

	stack := []incr.INode{node}
	seen := make(map[incr.Identifier]struct{})
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		id := n.Node().ID()
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}

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
	}
	return out
}

func sortByHeight[K Hashable, V any](nodes []Value[K, V]) {
	sort.SliceStable(nodes, func(i, j int) bool {
		return nodes[i].TopSortOrder() < nodes[j].TopSortOrder()
	})
}
