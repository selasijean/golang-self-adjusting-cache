package cache

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type cacheKey struct {
	t int
}

func (k cacheKey) Identifier() string {
	return strconv.Itoa(k.t)
}

func fnKey(t int) cacheKey {
	return cacheKey{t: t}
}

type testEvaluator struct {
	cache Cache[cacheKey, int]
}

func newEvaluator(cache Cache[cacheKey, int]) *testEvaluator {
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
	cache, evaluator, ctx := newEvaluatorCache()
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
	cache, evaluator, ctx := newEvaluatorCache()

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
	cache, evaluator, ctx := newEvaluatorCache()
	cache.WithCutoffFn(func(ctx context.Context, key cacheKey, previous, current int) (bool, error) {
		return previous == current, nil
	})

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
	cache, evaluator, ctx := newEvaluatorCache()
	externalCache := make(map[cacheKey]int)
	cache.WithWriteBackFn(func(ctx context.Context, key cacheKey, value int) error {
		externalCache[key] = value
		return nil
	})

	_, err := evaluator.identityFn(ctx, fnKey(1))
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	// verify that the value is written back to the external cache
	require.Equal(t, 1, externalCache[fnKey(1)])
}

func TestCacheNode_DirectDependents(t *testing.T) {
	cache, evaluator, ctx := newEvaluatorCache()

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
	cache, evaluator, ctx := newEvaluatorCache()

	maxT := 10
	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	require.Equal(t, int64(11), cache.Len()) // identifyFn(10) introduces 11 nodes into the cache (0 through 10)

	purgedCount := 0
	evalAtZero, ok := cache.Get(fnKey(0))
	require.True(t, ok)
	evalAtZero.OnPurged(func(ctx context.Context) {
		purgedCount++
	})

	cache.Purge(ctx, fnKey(0))
	require.Equal(t, int64(0), cache.Len()) // purge(0) clears all the direct and indirect dependents of identifyFn(0).
	require.Equal(t, 1, purgedCount)

	_, err = evaluator.identityFn(ctx, fnKey(maxT)) // reintroduce all the nodes into the cache and verify that the cache can be used again
	require.NoError(t, err)

	require.Equal(t, int64(11), cache.Len())
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		v, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_Clear(t *testing.T) {
	cache, evaluator, ctx := newEvaluatorCache()

	maxT := 10

	_, err := evaluator.identityFn(ctx, fnKey(maxT))
	require.NoError(t, err)

	require.Equal(t, int64(11), cache.Len())

	purgedCount := 0
	evalAtZero, ok := cache.Get(fnKey(0))
	require.True(t, ok)
	evalAtZero.OnPurged(func(ctx context.Context) {
		purgedCount++
	})

	cache.Clear(ctx)
	require.Equal(t, int64(0), cache.Len())
	require.Equal(t, 1, purgedCount)

	_, err = evaluator.identityFn(ctx, fnKey(maxT)) // reintroduce all the nodes into the cache and verify that the cache can be used again
	require.NoError(t, err)

	require.Equal(t, int64(11), cache.Len())
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		v, ok := cache.Get(key)
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_Parallel_Recompute(t *testing.T) {
	numCPU := runtime.NumCPU()
	cache, evaluator, ctx := newEvaluatorCache(OptUseParallelism(&numCPU))

	maxT := 10000
	keys := make([]cacheKey, 0, maxT)
	for i := 0; i < maxT; i++ {
		key := fnKey(i)
		keys = append(keys, key)

		_, err := evaluator.identityFn(ctx, key)
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
	cache, evaluator, ctx := newEvaluatorCache()

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
	cache, evaluator, ctx := newEvaluatorCache()

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
	original, evaluator, ctx := newEvaluatorCache()

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

type testSerializableKey struct {
	Key int
}

func (k testSerializableKey) Identifier() string {
	return strconv.Itoa(k.Key)
}

type testSerializableValue struct {
	Value string
}

func TestCache_MarshalUnmarshalJSON(t *testing.T) {
	valueFn := func(ctx context.Context, key testSerializableKey) (Entry[testSerializableKey, testSerializableValue], error) {
		return NewEntry(key, testSerializableValue{Value: fmt.Sprintf("val-%d", key.Key)}, nil), nil
	}

	cache := New(valueFn)
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		val, err := valueFn(ctx, testSerializableKey{Key: i})
		require.NoError(t, err)
		err = cache.Put(ctx, val)
		require.NoError(t, err)
	}

	b, err := cache.MarshalJSON()
	jsonStr := string(b)
	require.NotEmpty(t, jsonStr)
	require.NoError(t, err)

	copy := New(valueFn)
	err = copy.UnmarshalJSON(b)
	require.NoError(t, err)

	require.Equal(t, cache.Len(), copy.Len())

	for _, key := range cache.Keys() {
		cached, ok := cache.Get(key)
		require.True(t, ok)
		copyCached, ok := copy.Get(key)
		require.True(t, ok)
		require.Equal(t, cached.Value(), copyCached.Value())
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func identityValueFn(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
	return NewEntry(key, key.t, nil), nil
}

func newIdentityCache(opts ...CacheOption) (Cache[cacheKey, int], context.Context) {
	return New(identityValueFn, opts...), context.Background()
}

func newEvaluatorCache(opts ...CacheOption) (Cache[cacheKey, int], *testEvaluator, context.Context) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}
	cache := New(valueFn, opts...)
	evaluator = newEvaluator(cache)
	return cache, evaluator, context.Background()
}

func buildLinearChain(n int) (Cache[cacheKey, int], context.Context) {
	var evaluator *testEvaluator
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.identityFn(ctx, key)
	}
	c := New(valueFn)
	ctx := context.Background()
	evaluator = newEvaluator(c)

	for i := 0; i < n; i++ {
		_, _ = evaluator.identityFn(ctx, fnKey(i))
	}
	return c, ctx
}

func buildIndependentEntries(n int) (Cache[cacheKey, int], context.Context) {
	c, ctx := newIdentityCache()
	for i := 0; i < n; i++ {
		_ = c.Put(ctx, NewEntry(fnKey(i), i, nil))
	}
	return c, ctx
}

// Fan-in/out builders give their valueFn the same topology as the initial Puts;
// otherwise adjustDependencies would rewire edges on recompute and collapse the graph.

func buildFanOutCache(n int) (Cache[cacheKey, int], context.Context) {
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		if key.t == 0 {
			return NewEntry(key, key.t, nil), nil
		}
		return NewEntry(key, key.t, []cacheKey{fnKey(0)}), nil
	}
	c := New(valueFn)
	ctx := context.Background()

	_ = c.Put(ctx, NewEntry(fnKey(0), 0, nil))
	for i := 1; i <= n; i++ {
		_ = c.Put(ctx, NewEntry(fnKey(i), i, []cacheKey{fnKey(0)}))
	}
	return c, ctx
}

func buildFanInCache(n int) (Cache[cacheKey, int], context.Context) {
	rootDeps := make([]cacheKey, n)
	for i := 0; i < n; i++ {
		rootDeps[i] = fnKey(i)
	}
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		if key.t == n {
			return NewEntry(key, key.t, rootDeps), nil
		}
		return NewEntry(key, key.t, nil), nil
	}
	c := New(valueFn)
	ctx := context.Background()

	for i := 0; i < n; i++ {
		_ = c.Put(ctx, NewEntry(fnKey(i), i, nil))
	}
	_ = c.Put(ctx, NewEntry(fnKey(n), n, rootDeps))
	return c, ctx
}

// ---------------------------------------------------------------------------
// Edge Cases & Error Paths
// ---------------------------------------------------------------------------

func TestCache_EmptyCache(t *testing.T) {
	c, ctx := newIdentityCache()

	_, ok := c.Get(fnKey(0))
	require.False(t, ok)

	require.Equal(t, int64(0), c.Len())
	require.Empty(t, c.Keys())
	require.Empty(t, c.Values())

	c.Purge(ctx, fnKey(0))
	c.Clear(ctx)
	require.Equal(t, int64(0), c.Len())
}

func TestCache_SingleEntry(t *testing.T) {
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return NewEntry(key, key.t*10, nil), nil
	}
	c := New(valueFn)
	ctx := context.Background()

	err := c.Put(ctx, NewEntry(fnKey(1), 1, nil))
	require.NoError(t, err)

	v, ok := c.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 1, v.Value())

	err = c.Put(ctx, NewEntry(fnKey(1), 42, nil))
	require.NoError(t, err)
	v, ok = c.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 42, v.Value())

	err = c.Recompute(ctx, fnKey(1))
	require.NoError(t, err)
	v, ok = c.Get(fnKey(1))
	require.True(t, ok)
	require.Equal(t, 10, v.Value()) // valueFn returns key.t*10

	c.Purge(ctx, fnKey(1))
	require.Equal(t, int64(0), c.Len())
}

func TestCache_NewNilValueFnPanics(t *testing.T) {
	require.Panics(t, func() {
		New[cacheKey, int](nil)
	})
}

func TestCache_PutWithMissingDependency(t *testing.T) {
	c, ctx := newIdentityCache()
	err := c.Put(ctx, NewEntry(fnKey(1), 1, []cacheKey{fnKey(999)}))
	require.Error(t, err)
}

func TestCache_RecomputeNonExistentKey(t *testing.T) {
	c, ctx := newIdentityCache()
	err := c.Recompute(ctx, fnKey(0))
	require.Error(t, err)
	require.Contains(t, err.Error(), "key not found")
}

func TestCache_RecomputeValueFnError(t *testing.T) {
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return nil, errors.New("computation failed")
	}
	c := New(valueFn)
	ctx := context.Background()

	err := c.Put(ctx, NewEntry(fnKey(0), 0, nil))
	require.NoError(t, err)

	err = c.Recompute(ctx, fnKey(0))
	require.Error(t, err)
	require.Contains(t, err.Error(), "computation failed")
}

func TestCache_PurgeNonExistentKey(t *testing.T) {
	c, ctx := buildLinearChain(5)
	before := c.Len()
	c.Purge(ctx, fnKey(999))
	require.Equal(t, before, c.Len())
}

// Put called from inside valueFn during stabilization must add the entry
// without deadlocking or triggering a nested Stabilize.
func TestCache_ReentrantPutDuringStabilize(t *testing.T) {
	var c Cache[cacheKey, int]
	fired := false
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		if key.t == 0 && !fired {
			fired = true
			if err := c.Put(ctx, NewEntry(fnKey(100), 100, nil)); err != nil {
				return nil, err
			}
		}
		return NewEntry(key, key.t*10, nil), nil
	}
	c = New(valueFn)
	ctx := context.Background()

	require.NoError(t, c.Put(ctx, NewEntry(fnKey(0), 0, nil)))
	require.NoError(t, c.Recompute(ctx, fnKey(0)))
	require.True(t, fired)
	require.Equal(t, int64(2), c.Len())

	v, ok := c.Get(fnKey(100))
	require.True(t, ok)
	require.Equal(t, 100, v.Value())
}

func TestCache_WriteBackError(t *testing.T) {
	writeBackErr := errors.New("writeback failed")
	c, ctx := newIdentityCache()
	c.WithWriteBackFn(func(ctx context.Context, key cacheKey, value int) error {
		return writeBackErr
	})

	err := c.Put(ctx, NewEntry(fnKey(0), 0, nil))
	require.ErrorIs(t, err, writeBackErr)
}

// ---------------------------------------------------------------------------
// DAG Topology Tests
// ---------------------------------------------------------------------------

func TestCache_DiamondDependency(t *testing.T) {
	c, ctx := newIdentityCache()

	_ = c.Put(ctx, NewEntry(fnKey(0), 10, nil))
	_ = c.Put(ctx, NewEntry(fnKey(1), 20, nil))
	_ = c.Put(ctx, NewEntry(fnKey(2), 30, []cacheKey{fnKey(0), fnKey(1)}))
	_ = c.Put(ctx, NewEntry(fnKey(3), 40, []cacheKey{fnKey(2)}))

	require.Equal(t, int64(4), c.Len())

	updateCount := 0
	v3, _ := c.Get(fnKey(3))
	v3.OnUpdate(func(ctx context.Context) { updateCount++ })

	err := c.Put(ctx, NewEntry(fnKey(0), 99, nil))
	require.NoError(t, err)

	// D should have been updated exactly once (not once per path)
	require.Equal(t, 1, updateCount)
}

func TestCache_FanOut(t *testing.T) {
	n := 100
	c, ctx := buildFanOutCache(n)
	require.Equal(t, int64(n+1), c.Len())

	updateCounts := make([]int, n+1)
	for i := 1; i <= n; i++ {
		idx := i
		v, ok := c.Get(fnKey(i))
		require.True(t, ok)
		v.OnUpdate(func(ctx context.Context) { updateCounts[idx]++ })
	}

	err := c.Put(ctx, NewEntry(fnKey(0), 999, nil))
	require.NoError(t, err)

	for i := 1; i <= n; i++ {
		require.Equal(t, 1, updateCounts[i], "dependent %d should have been updated once", i)
	}
}

func TestCache_FanIn(t *testing.T) {
	n := 100
	c, ctx := buildFanInCache(n)
	require.Equal(t, int64(n+1), c.Len())

	fanInNode, ok := c.Get(fnKey(n))
	require.True(t, ok)
	require.Equal(t, n, fanInNode.Value())
	require.Equal(t, n, len(fanInNode.Dependencies()))

	updated := 0
	fanInNode.OnUpdate(func(ctx context.Context) { updated++ })

	for i := 0; i < n; i++ {
		require.NoError(t, c.Put(ctx, NewEntry(fnKey(i), 1000+i, nil)))
	}
	require.Equal(t, n, updated, "fan-in node should see one update per root change")
}

func TestCache_DiamondWithCutoff(t *testing.T) {
	c, ctx := newIdentityCache()
	c.WithCutoffFn(func(ctx context.Context, key cacheKey, previous, current int) (bool, error) {
		return previous == current, nil
	})

	_ = c.Put(ctx, NewEntry(fnKey(0), 10, nil))
	_ = c.Put(ctx, NewEntry(fnKey(1), 20, nil))
	_ = c.Put(ctx, NewEntry(fnKey(2), 30, []cacheKey{fnKey(0), fnKey(1)}))
	_ = c.Put(ctx, NewEntry(fnKey(3), 40, []cacheKey{fnKey(2)}))

	updateCount := 0
	v3, _ := c.Get(fnKey(3))
	v3.OnUpdate(func(ctx context.Context) { updateCount++ })

	// put same value for root — cutoff should stop propagation
	err := c.Put(ctx, NewEntry(fnKey(0), 10, nil))
	require.NoError(t, err)
	require.Equal(t, 0, updateCount)

	// put different value — should propagate
	err = c.Put(ctx, NewEntry(fnKey(0), 99, nil))
	require.NoError(t, err)
	require.Equal(t, 1, updateCount)
}

func TestCache_TopSortOrder(t *testing.T) {
	c, _ := buildLinearChain(10)

	for i := 0; i < 9; i++ {
		vi, ok := c.Get(fnKey(i))
		require.True(t, ok)
		vi1, ok := c.Get(fnKey(i + 1))
		require.True(t, ok)
		require.Less(t, vi.TopSortOrder(), vi1.TopSortOrder(),
			"key %d should have lower TopSortOrder than key %d", i, i+1)
	}
}

// ---------------------------------------------------------------------------
// Purge Variations
// ---------------------------------------------------------------------------

func TestCache_PurgeMiddleOfChain(t *testing.T) {
	c, ctx := buildLinearChain(10)
	require.Equal(t, int64(10), c.Len())

	// purge key 5 — should remove 5,6,7,8,9
	c.Purge(ctx, fnKey(5))

	for i := 0; i < 5; i++ {
		_, ok := c.Get(fnKey(i))
		require.True(t, ok, "key %d should still exist", i)
	}
	for i := 5; i < 10; i++ {
		_, ok := c.Get(fnKey(i))
		require.False(t, ok, "key %d should have been purged", i)
	}
	require.Equal(t, int64(5), c.Len())
}

func TestCache_PurgeLeafNode(t *testing.T) {
	c, ctx := buildLinearChain(5)
	require.Equal(t, int64(5), c.Len())

	c.Purge(ctx, fnKey(4))
	require.Equal(t, int64(4), c.Len())

	for i := 0; i < 4; i++ {
		_, ok := c.Get(fnKey(i))
		require.True(t, ok)
	}
	_, ok := c.Get(fnKey(4))
	require.False(t, ok)
}

func TestCache_PurgeMultipleRoots(t *testing.T) {
	c, ctx := newIdentityCache()

	// two independent chains: 0->1->2 and 10->11->12
	_ = c.Put(ctx, NewEntry(fnKey(0), 0, nil))
	_ = c.Put(ctx, NewEntry(fnKey(1), 1, []cacheKey{fnKey(0)}))
	_ = c.Put(ctx, NewEntry(fnKey(2), 2, []cacheKey{fnKey(1)}))
	_ = c.Put(ctx, NewEntry(fnKey(10), 10, nil))
	_ = c.Put(ctx, NewEntry(fnKey(11), 11, []cacheKey{fnKey(10)}))
	_ = c.Put(ctx, NewEntry(fnKey(12), 12, []cacheKey{fnKey(11)}))
	require.Equal(t, int64(6), c.Len())

	c.Purge(ctx, fnKey(0), fnKey(10))
	require.Equal(t, int64(0), c.Len())
}

// ---------------------------------------------------------------------------
// Copy & JSON
// ---------------------------------------------------------------------------

func TestCache_CopyIndependence(t *testing.T) {
	c, ctx := buildLinearChain(5)

	cp, err := c.Copy(ctx)
	require.NoError(t, err)

	err = c.Put(ctx, NewEntry(fnKey(0), 999, nil))
	require.NoError(t, err)

	origVal, _ := c.Get(fnKey(0))
	copyVal, _ := cp.Get(fnKey(0))
	require.Equal(t, 999, origVal.Value())
	require.Equal(t, 0, copyVal.Value(), "copy should be unaffected by original mutation")

	err = cp.Put(ctx, NewEntry(fnKey(0), 777, nil))
	require.NoError(t, err)

	origVal, _ = c.Get(fnKey(0))
	copyVal, _ = cp.Get(fnKey(0))
	require.Equal(t, 999, origVal.Value(), "original should be unaffected by copy mutation")
	require.Equal(t, 777, copyVal.Value())
}

func TestCache_MarshalUnmarshalJSON_WithDependencies(t *testing.T) {
	valueFn := func(ctx context.Context, key testSerializableKey) (Entry[testSerializableKey, testSerializableValue], error) {
		return NewEntry(key, testSerializableValue{Value: fmt.Sprintf("val-%d", key.Key)}, nil), nil
	}

	original := New(valueFn)
	ctx := context.Background()

	// build chain: 0 -> 1 -> 2
	k0, k1, k2 := testSerializableKey{Key: 0}, testSerializableKey{Key: 1}, testSerializableKey{Key: 2}
	_ = original.Put(ctx, NewEntry(k0, testSerializableValue{Value: "root"}, nil))
	_ = original.Put(ctx, NewEntry(k1, testSerializableValue{Value: "mid"}, []testSerializableKey{k0}))
	_ = original.Put(ctx, NewEntry(k2, testSerializableValue{Value: "leaf"}, []testSerializableKey{k1}))

	b, err := original.MarshalJSON()
	require.NoError(t, err)

	restored := New(valueFn)
	err = restored.UnmarshalJSON(b)
	require.NoError(t, err)
	require.Equal(t, original.Len(), restored.Len())

	for _, key := range original.Keys() {
		ov, _ := original.Get(key)
		rv, ok := restored.Get(key)
		require.True(t, ok)
		require.Equal(t, ov.Value(), rv.Value())
	}

	// verify propagation still works in restored cache
	err = restored.Put(ctx, NewEntry(k0, testSerializableValue{Value: "updated-root"}, nil))
	require.NoError(t, err)

	rv0, _ := restored.Get(k0)
	require.Equal(t, "updated-root", rv0.Value().Value)
}

// ---------------------------------------------------------------------------
// Handlers & Config
// ---------------------------------------------------------------------------

func TestCache_MultipleOnUpdateHandlers(t *testing.T) {
	c, ctx := buildLinearChain(2) // key0 -> key1

	v1, ok := c.Get(fnKey(1))
	require.True(t, ok)

	var order []int
	v1.OnUpdate(func(ctx context.Context) { order = append(order, 0) })
	v1.OnUpdate(func(ctx context.Context) { order = append(order, 1) })
	v1.OnUpdate(func(ctx context.Context) { order = append(order, 2) })

	err := c.Put(ctx, NewEntry(fnKey(0), 99, nil))
	require.NoError(t, err)

	require.Equal(t, []int{0, 1, 2}, order, "handlers should fire once each in registration order")
}

func TestCache_MultipleOnPurgedHandlers(t *testing.T) {
	c, ctx := buildLinearChain(2)

	v0, ok := c.Get(fnKey(0))
	require.True(t, ok)

	counts := [3]int{}
	v0.OnPurged(func(ctx context.Context) { counts[0]++ })
	v0.OnPurged(func(ctx context.Context) { counts[1]++ })
	v0.OnPurged(func(ctx context.Context) { counts[2]++ })

	c.Purge(ctx, fnKey(0))

	for i, cnt := range counts {
		require.Equal(t, 1, cnt, "handler %d should have fired once", i)
	}
}

func TestCache_WithHashFn(t *testing.T) {
	c, ctx := newIdentityCache()
	c.WithHashFn(func(s string) uintptr {
		h := fnv.New64a()
		_, _ = h.Write([]byte(s))
		return uintptr(h.Sum64())
	})

	for i := 0; i < 20; i++ {
		err := c.Put(ctx, NewEntry(fnKey(i), i, nil))
		require.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		v, ok := c.Get(fnKey(i))
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_DynamicDependencyChange(t *testing.T) {
	// on first call: key2 depends on key0
	// on recompute: key2 depends on key1
	switchDeps := false
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		if key.t == 2 && switchDeps {
			return NewEntry(key, 200, []cacheKey{fnKey(1)}), nil
		}
		return NewEntry(key, key.t, nil), nil
	}
	c := New(valueFn)
	ctx := context.Background()

	_ = c.Put(ctx, NewEntry(fnKey(0), 0, nil))
	_ = c.Put(ctx, NewEntry(fnKey(1), 1, nil))
	_ = c.Put(ctx, NewEntry(fnKey(2), 2, []cacheKey{fnKey(0)}))

	v2, _ := c.Get(fnKey(2))
	require.Equal(t, 2, v2.Value())
	require.Equal(t, []cacheKey{fnKey(0)}, v2.Dependencies())

	switchDeps = true
	err := c.Recompute(ctx, fnKey(2))
	require.NoError(t, err)

	v2, _ = c.Get(fnKey(2))
	require.Equal(t, 200, v2.Value())
	require.Equal(t, []cacheKey{fnKey(1)}, v2.Dependencies())
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

func TestCache_OptPreallocateSize(t *testing.T) {
	c, ctx := newIdentityCache(OptPreallocateSize(100))

	for i := 0; i < 50; i++ {
		err := c.Put(ctx, NewEntry(fnKey(i), i, nil))
		require.NoError(t, err)
	}
	require.Equal(t, int64(50), c.Len())
}

func TestCache_OptMaxHeightOfDependencyGraph(t *testing.T) {
	c, ctx := newIdentityCache(OptMaxHeightOfDependencyGraph(100))

	for i := 0; i < 10; i++ {
		var deps []cacheKey
		if i > 0 {
			deps = []cacheKey{fnKey(i - 1)}
		}
		err := c.Put(ctx, NewEntry(fnKey(i), i, deps))
		require.NoError(t, err)
	}

	require.Equal(t, int64(10), c.Len())
	for i := 0; i < 10; i++ {
		v, ok := c.Get(fnKey(i))
		require.True(t, ok)
		require.Equal(t, i, v.Value())
	}
}

func TestCache_OptUseParallelismNilCPU(t *testing.T) {
	c, ctx := newIdentityCache(OptUseParallelism(nil))

	for i := 0; i < 10; i++ {
		_ = c.Put(ctx, NewEntry(fnKey(i), i, nil))
	}

	err := c.Recompute(ctx, fnKey(0))
	require.NoError(t, err)
}

func TestCache_OptUseParallelismZeroCPU(t *testing.T) {
	zero := 0
	c, ctx := newIdentityCache(OptUseParallelism(&zero))

	for i := 0; i < 10; i++ {
		_ = c.Put(ctx, NewEntry(fnKey(i), i, nil))
	}

	err := c.Recompute(ctx, fnKey(0))
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

func TestCache_ConcurrentGets(t *testing.T) {
	c, _ := buildIndependentEntries(1000)

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				v, ok := c.Get(fnKey(i))
				require.True(t, ok)
				require.Equal(t, i, v.Value())
			}
		}()
	}
	wg.Wait()
}

func TestCache_ConcurrentRecompute(t *testing.T) {
	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return NewEntry(key, key.t*2, nil), nil
	}
	c := New(valueFn)
	ctx := context.Background()

	n := 500
	keys := make([]cacheKey, n)
	for i := 0; i < n; i++ {
		keys[i] = fnKey(i)
		_ = c.Put(ctx, NewEntry(fnKey(i), i, nil))
	}

	// recompute all keys in one batch (not from multiple goroutines, since
	// the underlying go-incr graph does not support concurrent Stabilize calls)
	err := c.Recompute(ctx, keys...)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				v, ok := c.Get(fnKey(i))
				require.True(t, ok)
				require.Equal(t, i*2, v.Value())
			}
		}()
	}
	wg.Wait()
}
