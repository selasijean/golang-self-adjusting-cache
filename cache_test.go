package cache

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

type cacheKey struct {
	t int
}

func fnKey(t int) cacheKey {
	return cacheKey{t: t}
}

type testEvaluator struct {
	cache *Cache[cacheKey, int]
}

func newEvaluator(cache *Cache[cacheKey, int]) *testEvaluator {
	return &testEvaluator{
		cache: cache,
	}
}

// identity(x) = identity(x-1) + 1 where identity(0) = 0.
func (e *testEvaluator) identityFn(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
	cached, ok := e.cache.Get(key)
	if ok {
		return cached, nil
	}

	out := NewEntry(key, 0, nil)
	t := key.t
	if t > 0 {
		tMinus := fnKey(t - 1)
		r, err := e.identityFn(ctx, tMinus)
		if err != nil {
			return nil, err
		}
		out = NewEntry(key, r.Value()+1, []cacheKey{fnKey(t - 1)})
	}

	err := e.cache.Put(ctx, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// complex(x) = identity(x-1) + identity(x-2).
func (e *testEvaluator) complexFn(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
	cached, ok := e.cache.Get(key)
	if ok {
		return NewEntry(key, cached.Value(), cached.Dependencies()), nil
	}

	out := NewEntry(key, 0, nil)
	t := key.t
	if t > 0 {
		t1 := fnKey(t - 1)
		r1, err := e.identityFn(ctx, t1)
		if err != nil {
			return nil, err
		}
		t2 := fnKey(t - 2)
		r2, err := e.identityFn(ctx, t2)
		if err != nil {
			return nil, err
		}

		out = NewEntry(key, r1.Value()+r2.Value(), []cacheKey{t1, t2})
	}

	err := e.cache.Put(ctx, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func TestCache_Simple(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)
	maxT := 10

	// evaluate at each i and put into the cache. This builds dependency relationships of cached values induced by the identityFn.
	for i := 0; i <= maxT; i++ {
		key := fnKey(i)
		_, err := evaluator.identityFn(ctx, key)
		require.NoError(t, err)

		cached, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i, cached.Value())
	}

	key := fnKey(0)
	// override identityFn(0) with 1. This will cause a ripple effect on all cached values that depend on identityFn(0).
	err := cache.Put(ctx, NewEntry(key, 1, nil))
	require.NoError(t, err)
	cached, ok := cache.Get(key)
	require.True(t, ok)
	require.Equal(t, 1, cached.Value())

	// verify that the value at i is updated to i+1 because of the override above.
	for i := 1; i <= maxT; i++ {
		key := fnKey(i)
		cached, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i+1, cached.Value())
	}
}

func TestCache_Recompute(t *testing.T) {
	var evaluator *testEvaluator
	var fnOverride *cacheKey
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		if fnOverride != nil && key == *fnOverride {
			return evaluator.complexFn(ctx, key)
		}
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	four := fnKey(4)

	_, err := evaluator.identityFn(ctx, four)
	require.NoError(t, err)

	eval, ok := cache.Get(four)
	require.True(t, ok)
	require.Equal(t, 4, eval.Value())

	fnOverride = &four
	err = cache.Recompute(ctx, four)
	require.NoError(t, err)

	eval, ok = cache.Get(fnKey(4))
	require.True(t, ok)
	require.Equal(t, 5, eval.Value()) // since valueFn is overridden to be complexFn(4) = identityFn(3) + identityFn(2) = 3 + 2 = 5.

	// override identityFn(0) to 1
	err = cache.Put(ctx, NewEntry(fnKey(0), 1, nil))
	require.NoError(t, err)

	eval, ok = cache.Get(fnKey(4))
	require.True(t, ok)
	require.Equal(t, 7, eval.Value()) // since identityFn(0) = 1, complexFn(4) = identityFn(3) + identityFn(2) = 4 + 3 = 7.
}

func TestCache_WithoutCutoff(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	_, err := evaluator.identityFn(ctx, fnKey(1))
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	updatedCount := 0
	evalAtOne.OnUpdate(func(ctx context.Context) {
		updatedCount++
	})

	numTries := 3
	for i := 1; i <= numTries; i++ {
		key := fnKey(0)
		// override evalAtZero to have value 1
		err = cache.Put(ctx, NewEntry(key, 1, nil))
		require.NoError(t, err)
		evalAtZero, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, 1, evalAtZero.Value())

		// update to evalAtZero should trigger an update to evalAtOne
		require.Equal(t, i, updatedCount)
	}
}

func TestCache_WithCutoff(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cutoffFn := func(ctx context.Context, previous int, current int) (bool, error) {
		return previous == current, nil
	}

	cache := New(valueFn).WithCutoffFn(cutoffFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	_, err := evaluator.identityFn(ctx, fnKey(1))
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	updatedCount := 0
	evalAtOne.OnUpdate(func(ctx context.Context) {
		updatedCount++
	})

	numTries := 5
	for i := 1; i <= numTries; i++ {
		key := fnKey(0)
		// override evalAtZero to have value 1
		err = cache.Put(ctx, NewEntry(key, 1, nil))
		require.NoError(t, err)
		evalAtZero, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, 1, evalAtZero.Value())

		evalAtOne, ok := cache.Get(fnKey(1))
		require.True(t, ok)
		require.Equal(t, 2, evalAtOne.Value())

		// update to evalAtZero should trigger an update to evalAtOne only once since cutoff kicks in after the first update
		require.Equal(t, 1, updatedCount)
	}

	// override evalAtZero to have value 0
	err = cache.Put(ctx, NewEntry(fnKey(0), 0, nil))
	require.NoError(t, err)

	evalAtZero, ok := cache.Get(fnKey(0))
	require.True(t, ok)
	require.Equal(t, 0, evalAtZero.Value())

	evalAtOne, ok = cache.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	// update to evalAtZero should trigger an update to evalAtOne
	require.Equal(t, 2, updatedCount)
}

func TestCache_WithWriteBack(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	externalCache := make(map[cacheKey]int)
	writeBackFn := func(ctx context.Context, key cacheKey, value int) error {
		externalCache[key] = value
		return nil
	}

	cache := New(valueFn).WithWriteBackFn(writeBackFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	_, err := evaluator.identityFn(ctx, fnKey(1))
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	// verify that the value is written back to the external cache
	require.Equal(t, 1, externalCache[fnKey(1)])
}

func TestCacheNode_DirectDependents(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		node, ok := cache.Get(key)
		require.True(t, ok)

		dependents := node.DirectDependents()
		require.Equal(t, 1, len(dependents))
		require.Equal(t, fnKey(i+1), dependents[0])
	}
}

func TestCache_Purge(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	require.Equal(t, 11, cache.Len()) // identifyFn(10) introduces 11 nodes into the cache (0 through 10)

	purgedCount := 0
	evalAtZero, ok := cache.Get(fnKey(0))
	require.True(t, ok)
	evalAtZero.OnPurged(func(ctx context.Context) {
		purgedCount++
	})

	cache.Purge(ctx, fnKey(0))
	require.Equal(t, 0, cache.Len()) // purge(0) clears all the direct and indirect dependents of identifyFn(0).
	require.Equal(t, 1, purgedCount)

	_, err = evaluator.identityFn(ctx, fnKey(maxT)) // reintroduce all the nodes into the cache and verify that the cache can be used again
	require.NoError(t, err)

	require.Equal(t, 11, cache.Len())
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		v, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_Clear(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10

	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	require.Equal(t, 11, cache.Len())

	purgedCount := 0
	evalAtZero, ok := cache.Get(fnKey(0))
	require.True(t, ok)
	evalAtZero.OnPurged(func(ctx context.Context) {
		purgedCount++
	})

	cache.Clear(ctx)
	require.Equal(t, 0, cache.Len())
	require.Equal(t, 1, purgedCount)

	_, err = evaluator.identityFn(ctx, fnKey(maxT)) // reintroduce all the nodes into the cache and verify that the cache can be used again
	require.NoError(t, err)

	require.Equal(t, 11, cache.Len())
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		v, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_Parallel_Recompute(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	numCPU := runtime.NumCPU()
	cache := New(valueFn, OptUseParallelism(&numCPU))
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10000
	keys := make([]cacheKey, 0, maxT)
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		keys = append(keys, key)

		_, err := valueFn(ctx, key)
		require.NoError(t, err)
	}

	err := cache.Recompute(ctx, keys...)
	require.NoError(t, err)

	for _, key := range keys {
		cached, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, key.t, cached.Value())
	}
}

func TestCache_Keys(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	keys := cache.Keys()
	require.Equal(t, maxT+1, len(keys))
	for i := 0; i <= maxT; i++ {
		require.Contains(t, keys, fnKey(i))
	}
}

func TestCache_Values(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	cache := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(cache)

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	values := cache.Values()
	require.Equal(t, maxT+1, len(values))
	for i := 0; i <= maxT; i++ {
		value := values[i]
		expected := value.Key().t
		require.Equal(t, expected, value.Value())
	}
}

func TestCache_Copy(t *testing.T) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}

	original := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(original)

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	copy, err := original.Copy(ctx)
	require.NoError(t, err)
	require.Equal(t, original.Len(), copy.Len())

	require.True(t, copy != original)

	for _, key := range original.Keys() {
		cached, ok := original.Get(key)
		require.True(t, ok)
		copyCached, ok := copy.Get(key)
		require.True(t, ok)
		require.Equal(t, cached.Value(), copyCached.Value())
	}
}
