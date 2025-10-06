package lazy_pool

@(private = "file")
ParallelReduceCtx :: struct($T: typeid, $S: typeid) {
	items:      []T,
	partials:   []S,
	chunk_size: int,
	reduce_fn:  proc(acc: S, item: T) -> S,
	initial:    S,
}

parallel_reduce :: proc {
	_parallel_reduce,
	_parallel_reduce_merge,
}

@(private = "file")
_parallel_reduce :: proc(
	p: ^LazyPool,
	items: []$T,
	chunk_size: int,
	reduce_fn: proc(acc: T, item: T) -> T,
	initial: T,
	allocator := context.allocator,
) -> T {
	count := len(items)
	if count == 0 {
		return initial
	}

	num_chunks := (count + chunk_size - 1) / chunk_size
	partials := make([]T, num_chunks, allocator)
	defer delete(partials)

	ctx := ParallelReduceCtx(T, T) {
		items      = items,
		partials   = partials,
		chunk_size = chunk_size,
		reduce_fn  = reduce_fn,
		initial    = initial,
	}
	run_chunk := proc(ctx: ^ParallelReduceCtx(T, T), start: int, end: int) {
		chunk_index := start / ctx.chunk_size
		acc := ctx.initial
		for i in start ..< end {
			acc = ctx.reduce_fn(acc, ctx.items[i])
		}
		ctx.partials[chunk_index] = acc
	}

	schedule_parallel_chunks(p, count, chunk_size, &ctx, run_chunk, allocator)

	result := initial
	for i in 0 ..< len(partials) {
		result = reduce_fn(result, partials[i])
	}
	return result
}

@(private = "file")
_parallel_reduce_merge :: proc(
	p: ^LazyPool,
	items: []$T,
	chunk_size: int,
	reduce_fn: proc(acc: $S, item: T) -> S,
	merge_fn: proc(item1: S, item2: S) -> S,
	initial: S,
	allocator := context.allocator,
) -> S {
	count := len(items)
	if count == 0 {
		return initial
	}

	num_chunks := (count + chunk_size - 1) / chunk_size
	partials := make([]S, num_chunks, allocator)
	defer delete(partials)

	ctx := ParallelReduceCtx(T, S) {
		items      = items,
		partials   = partials,
		chunk_size = chunk_size,
		reduce_fn  = reduce_fn,
		initial    = initial,
	}
	run_chunk := proc(ctx: ^ParallelReduceCtx(T, S), start: int, end: int) {
		chunk_index := start / ctx.chunk_size
		acc := ctx.initial
		for i in start ..< end {
			acc = ctx.reduce_fn(acc, ctx.items[i])
		}
		ctx.partials[chunk_index] = acc
	}

	schedule_parallel_chunks(p, count, chunk_size, &ctx, run_chunk, allocator)

	result := initial
	for i in 0 ..< len(partials) {
		result = merge_fn(result, partials[i])
	}
	return result
}
