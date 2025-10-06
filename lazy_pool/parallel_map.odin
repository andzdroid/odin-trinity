package lazy_pool

@(private = "file")
ParallelMapCtx :: struct($T: typeid, $S: typeid) {
	items: []T,
	fn:    proc(item: T) -> S,
}

parallel_map :: proc(
	pool: ^LazyPool,
	items: []$T,
	chunk_size: int,
	fn: proc(item: T) -> $S,
	allocator := context.allocator,
) {
	ctx := ParallelMapCtx(T, S) {
		items = items,
		fn    = fn,
	}
	run_chunk := proc(ctx: ^ParallelMapCtx(T, S), start: int, end: int) {
		for index in start ..< end {
			ctx.items[index] = ctx.fn(ctx.items[index])
		}
	}
	schedule_parallel_chunks(pool, len(items), chunk_size, &ctx, run_chunk, allocator)
}
