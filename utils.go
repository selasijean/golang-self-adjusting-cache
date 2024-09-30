package cache

import (
	"sort"
	"sync"

	"github.com/wcharczuk/go-incr"
)

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

func invalidateNodes[K comparable, V any](nodes []*cacheNode[K, V]) {
	for _, node := range nodes {
		node.MarkAsInvalid()
	}
}

func validateNodes[K comparable, V any](nodes []*cacheNode[K, V]) {
	for _, node := range nodes {
		node.MarkAsValid()
	}
}

func withTemporaryInvalidation[K comparable, V any](nodes []*cacheNode[K, V], fn func() error) error {
	invalidateNodes(nodes)
	defer validateNodes(nodes)

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
