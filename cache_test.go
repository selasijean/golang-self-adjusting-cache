package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type cacheKey struct {
	t int
}

type testEvaluator struct {
	cache Cache[cacheKey, int]
}

func newEvaluator(cache Cache[cacheKey, int]) *testEvaluator {
	return &testEvaluator{
		cache: cache,
	}
}

func (e *testEvaluator) plusOne(key cacheKey) (Entry[cacheKey, int], error) {
	cached, ok := e.cache.Get(key)
	if ok {
		return NewEntry(key, cached.Value(), cached.Dependencies()), nil
	}

	out := NewEntry(key, 0, nil)
	t := key.t
	if t > 0 {
		tMinus := cacheKey{t: t - 1}
		r, err := e.plusOne(tMinus)
		if err != nil {
			return nil, err
		}
		out = NewEntry(key, r.Value()+1, []Entry[cacheKey, int]{r})
	}

	return out, nil
}

const (
	maxT int = 10
)

func TestCache_Simple(t *testing.T) {
	ctx := context.Background()

	var evaluator *testEvaluator

	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.plusOne(key)
	}

	cache := New(valueFn)
	evaluator = newEvaluator(cache)

	// evaluate at i and put into cache and build dependency graph of cached values given the formula, f(x) = f(x-1) + 1
	// note that the dependency relations of cached values returned by the evaluator.
	for i := 0; i <= maxT; i++ {
		result, err := evaluator.plusOne(cacheKey{t: i})
		require.NoError(t, err)

		err = evaluator.cache.Put(ctx, result)
		require.NoError(t, err)

		cached, ok := cache.Get(cacheKey{t: i})
		require.True(t, ok)
		require.Equal(t, i, cached.Value())
	}

	// override f(0) with 1. since we maintain the same dependency relations, the value at i should be updated to i+1
	err := cache.Put(ctx, NewEntry(cacheKey{t: 0}, 1, nil))
	require.NoError(t, err)
	cached, ok := cache.Get(cacheKey{t: 0})
	require.True(t, ok)
	require.Equal(t, 1, cached.Value())

	// verify that the value at i is updated to i+1 because of the override above
	for i := 1; i <= maxT; i++ {
		cached, ok := cache.Get(cacheKey{t: i})
		require.True(t, ok)
		require.Equal(t, i+1, cached.Value())
	}
}

func TestCache_WithoutCutoff(t *testing.T) {
	ctx := context.Background()

	var evaluator *testEvaluator

	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.plusOne(key)
	}

	cache := New(valueFn)
	evaluator = newEvaluator(cache)

	result, err := evaluator.plusOne(cacheKey{t: 1})
	require.NoError(t, err)

	err = evaluator.cache.Put(ctx, result)
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(cacheKey{t: 1})
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	updatedCount := 0
	evalAtOne.OnUpdate(func(ctx context.Context) {
		updatedCount++
	})

	numTries := 3
	for i := 1; i <= numTries; i++ {
		// override evalAtZero to have value 1
		err = cache.Put(ctx, NewEntry(cacheKey{t: 0}, 1, nil))
		require.NoError(t, err)
		evalAtZero, ok := cache.Get(cacheKey{t: 0})
		require.True(t, ok)
		require.Equal(t, 1, evalAtZero.Value())

		// update to evalAtZero should trigger an update to evalAtOne
		require.Equal(t, i, updatedCount)
	}
}

func TestCache_WithCutoff(t *testing.T) {
	ctx := context.Background()

	var evaluator *testEvaluator

	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.plusOne(key)
	}

	cutoffFn := func(ctx context.Context, previous int, current int) (bool, error) {
		return previous == current, nil
	}

	cache := New(valueFn).WithCutoffFn(cutoffFn)
	evaluator = newEvaluator(cache)

	result, err := evaluator.plusOne(cacheKey{t: 1})
	require.NoError(t, err)

	err = evaluator.cache.Put(ctx, result)
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(cacheKey{t: 1})
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	updatedCount := 0
	evalAtOne.OnUpdate(func(ctx context.Context) {
		updatedCount++
	})

	numTries := 5
	for i := 1; i <= numTries; i++ {
		// override evalAtZero to have value 1
		err = cache.Put(ctx, NewEntry(cacheKey{t: 0}, 1, nil))
		require.NoError(t, err)
		evalAtZero, ok := cache.Get(cacheKey{t: 0})
		require.True(t, ok)
		require.Equal(t, 1, evalAtZero.Value())

		// update to evalAtZero should trigger an update to evalAtOne only once since cutoff kicks in after the first update
		require.Equal(t, 1, updatedCount)
	}

	// override evalAtZero to have value 0
	err = cache.Put(ctx, NewEntry(cacheKey{t: 0}, 0, nil))
	require.NoError(t, err)
	evalAtZero, ok := cache.Get(cacheKey{t: 0})
	require.True(t, ok)
	require.Equal(t, 0, evalAtZero.Value())

	// update to evalAtZero should trigger an update to evalAtOne
	require.Equal(t, 2, updatedCount)
}

func TestCache_WithWriteBack(t *testing.T) {
	ctx := context.Background()

	var evaluator *testEvaluator

	valueFn := func(ctx context.Context, key cacheKey) (Entry[cacheKey, int], error) {
		return evaluator.plusOne(key)
	}

	externalCache := make(map[cacheKey]int)
	writeBackFn := func(ctx context.Context, key cacheKey, value int) error {
		externalCache[key] = value
		return nil
	}

	cache := New(valueFn).WithWriteBackFn(writeBackFn)
	evaluator = newEvaluator(cache)

	result, err := evaluator.plusOne(cacheKey{t: 1})
	require.NoError(t, err)

	err = evaluator.cache.Put(ctx, result)
	require.NoError(t, err)

	evalAtOne, ok := cache.Get(cacheKey{t: 1})
	require.True(t, ok)
	require.Equal(t, 1, evalAtOne.Value())

	// verify that the value is written back to the external cache
	require.Equal(t, 1, externalCache[cacheKey{t: 1}])
}
