# Self Adjusting Cache

This is a cache designed for use cases where you have complex, interdependent cached data that need to be efficiently recomputed when there are changes to related entries in the cache.

In the case where the value function used to populate the cache produces the same output for a given set of inputs, you can optimize the cache's performance by preempting change propagation using cutoff conditions. That is, when a cache entry's value changes, the cache uses cutoff conditions to determine if the change is "significant enough" to propagate further and trigger recomputations of dependent entries. For example, if a numeric value in the cache changes from `3.0001` to `3.0002`, and the cutoff is defined to ignore small differences, then the update will not propagate to other dependent cache entries because it's considered negligible. This can help prevent unnecessary work and can lead to efficient change propagation.

To summarize, this package provides a cache implementation that offers the following:

- **Dependency Tracking**: Automatically tracks dependencies between cached values and allows for dynamic updates to the relationships between cache entries.
- **Efficient Recomputation**: Uses incremental computation to recompute only the necessary parts of cache when cache entries change and allows for conditional termination of propagations via cutoff conditions.

# Usage

A mini-worked example:

```go
import "github.com/selasijean/self-adjusting-cache"

...
func (e *evaluator) identityFn(ctx context.Context, t int) (Entry[int, int], error) {
	cached, ok := e.cache.Get(t)
	if ok {
		return cached, nil
	}

	out := NewEntry(t, 0, nil)
	if t > 0 {
		tMinus := t-1
		r, err := e.identityFn(ctx, tMinus)
		if err != nil {
			return nil, err
		}
		out = NewEntry(t, r.Value()+1, []int{tMinus})
	}

	err := e.cache.Put(ctx, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

valueFn := func(ctx context.Context, t int) (Entry[int, int], error) {
	return evaluator.identityFn(ctx, t) // where identity(t) = identity(t-1) + 1 where identity(0) = 0.
}

cache := cache.New(valueFn)

_, err := evaluator.identityFn(ctx, 4)
require.NoError(t, err)

cached, ok := cache.Get(4)
require.True(t, ok)
require.Equal(t, 4, cached.Value())

// setting identityFn(0) to 1 by overriding the value in the cache.
err = cache.Put(ctx, NewEntry(0, 1, nil))
require.NoError(t, err)

// all cached values that depend on identityFn(0) should be recomputed and their values offset by 1.
for i := 1; i <= 4; i++ {
	cached, ok = cache.Get(i)
	require.True(t, ok)
	require.Equal(t, i+1, cached.Value())
}

```

# A note on Parallelism

`Put` adds to the cache serially, building the internal graph that tracks the relationships between cached values. When parallelism is enabled, it only works with `Recompute` because the cache at that point has sufficient information from its internal graph to infer which entries are independent from one another and hence can be recomputed in parallel.

In certain cases, Parallel mode may be slower to recompute since locks have to be acquired and shared state managed carefully. You should only reach to use Parallel mode if the computation involved in re-evaluating a cache value is large relative to that overhead of managing locks and shared state.
