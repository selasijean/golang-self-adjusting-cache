package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"testing"
)

type benchKey struct {
	t int
}

func (k benchKey) Identifier() string {
	return strconv.Itoa(k.t)
}

func bKey(t int) benchKey {
	return benchKey{t: t}
}

// The valueFn must return the same topology the cache was built with; Put's
// stabilize path re-invokes it on dependents and adjustDependencies rewires
// edges to match whatever deps it returns.

func valueFnLinear() func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
	return func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
		var deps []benchKey
		if key.t > 0 {
			deps = []benchKey{bKey(key.t - 1)}
		}
		return NewEntry(key, key.t, deps), nil
	}
}

func valueFnIndependent() func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
	return func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
		return NewEntry(key, key.t, nil), nil
	}
}

func valueFnFanOut() func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
	return func(ctx context.Context, key benchKey) (Entry[benchKey, int], error) {
		if key.t == 0 {
			return NewEntry(key, key.t, nil), nil
		}
		return NewEntry(key, key.t, []benchKey{bKey(0)}), nil
	}
}

func benchLinearChain(b *testing.B, n int) Cache[benchKey, int] {
	c := New(valueFnLinear())
	ctx := context.Background()
	for i := 0; i < n; i++ {
		var deps []benchKey
		if i > 0 {
			deps = []benchKey{bKey(i - 1)}
		}
		if err := c.Put(ctx, NewEntry(bKey(i), i, deps)); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

func benchIndependent(b *testing.B, n int) Cache[benchKey, int] {
	c := New(valueFnIndependent())
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := c.Put(ctx, NewEntry(bKey(i), i, nil)); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

func benchFanOut(b *testing.B, n int) Cache[benchKey, int] {
	c := New(valueFnFanOut())
	ctx := context.Background()
	if err := c.Put(ctx, NewEntry(bKey(0), 0, nil)); err != nil {
		b.Fatal(err)
	}
	for i := 1; i <= n; i++ {
		if err := c.Put(ctx, NewEntry(bKey(i), i, []benchKey{bKey(0)})); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

func benchKeys(n int) []benchKey {
	keys := make([]benchKey, n)
	for i := 0; i < n; i++ {
		keys[i] = bKey(i)
	}
	return keys
}

var benchScales = []int{10, 100, 1_000}

// ---------------------------------------------------------------------------
// Core Operations
// ---------------------------------------------------------------------------

func BenchmarkPut_Independent(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				benchIndependent(b, n)
			}
		})
	}
}

func BenchmarkPut_LinearChain(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				benchLinearChain(b, n)
			}
		})
	}
}

func BenchmarkGet(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := benchIndependent(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for i := 0; i < n; i++ {
					c.Get(bKey(i))
				}
			}
		})
	}
}

func BenchmarkRecompute_SingleKey(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("chain=%d", n), func(b *testing.B) {
			c := benchLinearChain(b, n)
			ctx := context.Background()
			leaf := bKey(n - 1)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Recompute(ctx, leaf); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkRecompute_AllKeys(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := benchIndependent(b, n)
			ctx := context.Background()
			keys := benchKeys(n)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Recompute(ctx, keys...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Propagation
// ---------------------------------------------------------------------------

func BenchmarkPropagation_LinearChain(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("depth=%d", n), func(b *testing.B) {
			c := benchLinearChain(b, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Put(ctx, NewEntry(bKey(0), iter, nil)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPropagation_FanOut(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("width=%d", n), func(b *testing.B) {
			c := benchFanOut(b, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Put(ctx, NewEntry(bKey(0), iter, nil)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Cutoff
// ---------------------------------------------------------------------------

func BenchmarkCutoff_LinearChain(b *testing.B) {
	cutoffFn := func(ctx context.Context, key benchKey, prev, curr int) (bool, error) {
		return prev == curr, nil
	}
	for _, n := range []int{100, 1_000} {
		b.Run(fmt.Sprintf("depth=%d/no_cutoff", n), func(b *testing.B) {
			c := benchLinearChain(b, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Put(ctx, NewEntry(bKey(0), iter, nil)); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("depth=%d/with_cutoff", n), func(b *testing.B) {
			c := benchLinearChain(b, n).WithCutoffFn(cutoffFn)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Put(ctx, NewEntry(bKey(0), 0, nil)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Parallelism
// ---------------------------------------------------------------------------

func BenchmarkRecompute_Parallel(b *testing.B) {
	for _, n := range []int{100, 1_000} {
		b.Run(fmt.Sprintf("n=%d/sequential", n), func(b *testing.B) {
			c := benchIndependent(b, n)
			ctx := context.Background()
			keys := benchKeys(n)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Recompute(ctx, keys...); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("n=%d/parallel", n), func(b *testing.B) {
			numCPU := runtime.NumCPU()
			c := New(valueFnIndependent(), OptUseParallelism(&numCPU))
			ctx := context.Background()
			for i := 0; i < n; i++ {
				if err := c.Put(ctx, NewEntry(bKey(i), i, nil)); err != nil {
					b.Fatal(err)
				}
			}
			keys := benchKeys(n)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if err := c.Recompute(ctx, keys...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Structural Operations
// ---------------------------------------------------------------------------

func BenchmarkPurge_Root(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("depth=%d", n), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				c := benchLinearChain(b, n)
				c.Purge(ctx, bKey(0))
			}
		})
	}
}

func BenchmarkPurge_Leaf(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("depth=%d", n), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				c := benchLinearChain(b, n)
				c.Purge(ctx, bKey(n-1))
			}
		})
	}
}

func BenchmarkClear(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				c := benchIndependent(b, n)
				c.Clear(ctx)
			}
		})
	}
}

func BenchmarkCopy(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := benchLinearChain(b, n)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if _, err := c.Copy(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

type benchSerKey struct {
	Key int `json:"key"`
}

func (k benchSerKey) Identifier() string {
	return strconv.Itoa(k.Key)
}

type benchSerVal struct {
	Value string `json:"value"`
}

func benchSerValueFn(ctx context.Context, key benchSerKey) (Entry[benchSerKey, benchSerVal], error) {
	return NewEntry(key, benchSerVal{Value: fmt.Sprintf("v-%d", key.Key)}, nil), nil
}

func benchSerCache(b *testing.B, n int) Cache[benchSerKey, benchSerVal] {
	c := New(benchSerValueFn)
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if err := c.Put(ctx, NewEntry(benchSerKey{Key: i}, benchSerVal{Value: fmt.Sprintf("v-%d", i)}, nil)); err != nil {
			b.Fatal(err)
		}
	}
	return c
}

func BenchmarkMarshalJSON(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			c := benchSerCache(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				if _, err := c.MarshalJSON(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkUnmarshalJSON(b *testing.B) {
	for _, n := range benchScales {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			data, err := json.Marshal(benchSerCache(b, n))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				target := New(benchSerValueFn)
				if err := target.UnmarshalJSON(data); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
