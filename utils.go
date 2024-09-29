package cache

import (
	"sort"
	"sync"
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
