# Self Adjusting Cache

This is an online cache designed for use cases where you have complex, interdependent cached data that need to be efficiently recomputed.

Here, the entries in the cache are simple key-value pairs that are generated via deterministic functions of other values in the cache. Since deterministic functions always produce the same output for a given set of inputs, this property grants us the ability to optimize the cache's performance by preempting change propagation using cutoff conditions. That is, when a cache entry's value changes, the cache uses cutoff conditions to determine if the change is "significant enough" to propagate further to dependents. 

For example, if a numeric value in the cache changes from `3.0001` to `3.0002`, and the cutoff is defined to ignore small differences, then the update will not propagate to other dependent cache entries because it's considered negligible. This can help prevent unnecessary work and can lead to more efficient updates.

To summarize, this package provides a cache implementation that offers the following:

- **Dependency Tracking**: Automatically tracks dependencies between cached values.
- **Efficient Recomputation**: Uses incremental computation to recompute only the necessary parts of cache when cache entries change.
- **Cutoff Conditions**: Allows for conditional termination of change propagations.
- **Observability**: Provides mechanisms to observe and respond to cache updates.
