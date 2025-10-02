package lazy_pool

import notifier "../notifier"
import "core:time"

ParallelForJob :: struct($T: typeid) {
	fn:    proc(_: ^T, start: int),
	ctx:   ^T,
	start: int,
}

make_parallel_for_job :: proc(wrapped: ^ParallelForJob($T)) -> Job {
	run := proc(wrapped: ^ParallelForJob(T)) {
		wrapped.fn(wrapped.ctx, wrapped.start)
	}
	return make_job(run, wrapped)
}

ChunkDispatchCtx :: struct($U: typeid) {
	run_chunk:  proc(ctx: ^U, start: int, end: int),
	user_ctx:   ^U,
	count:      int,
	chunk_size: int,
	remaining:  ^i64,
	done:       ^notifier.Notifier,
}

schedule_parallel_chunks :: proc(
	pool: ^LazyPool,
	count: int,
	chunk_size: int,
	user_ctx: ^$U,
	run_chunk: proc(ctx: ^U, start: int, end: int),
	allocator := context.allocator,
) {
	if count <= 0 {
		return
	}

	chunk_size := max(1, chunk_size)
	remaining := i64((count + chunk_size - 1) / chunk_size)
	done: notifier.Notifier

	dispatch_ctx := ChunkDispatchCtx(U) {
		run_chunk  = run_chunk,
		user_ctx   = user_ctx,
		count      = count,
		chunk_size = chunk_size,
		remaining  = &remaining,
		done       = &done,
	}

	submit_chunk := proc(ctx: ^ChunkDispatchCtx(U), start: int) {
		end := min(start + ctx.chunk_size, ctx.count)
		ctx.run_chunk(ctx.user_ctx, start, end)
		if add(ctx.remaining, -1, .Acq_Rel) == 1 {
			notifier.notify_one(ctx.done)
		}
	}

	num_chunks := (count + chunk_size - 1) / chunk_size
	wrappers := make([]ParallelForJob(ChunkDispatchCtx(U)), num_chunks, allocator)
	defer delete(wrappers)

	for chunk_start := 0; chunk_start < count; chunk_start += chunk_size {
		wrapper_index := chunk_start / chunk_size
		wrappers[wrapper_index] = ParallelForJob(ChunkDispatchCtx(U)) {
			fn    = submit_chunk,
			ctx   = &dispatch_ctx,
			start = chunk_start,
		}
		job := make_parallel_for_job(&wrappers[wrapper_index])
		if !worker_submit(job) {
			for !pool_submit(pool, job) {
				time.sleep(10 * time.Microsecond)
			}
		}
	}

	if load(dispatch_ctx.remaining, .Acquire) == 0 {
		return
	}

	// Wait until last chunk completes
	epoch := notifier.prepare_wait(&done)
	if load(dispatch_ctx.remaining, .Acquire) == 0 {
		notifier.cancel_wait(&done)
		return
	}
	notifier.commit_wait(&done, epoch)
}

ElementForCtx :: struct {
	per_index_callback: proc(i: int),
}

element_chunk_runner :: proc(ctx: ^ElementForCtx, start: int, end: int) {
	for index in start ..< end {
		ctx.per_index_callback(index)
	}
}

parallel_for_simple :: proc(
	pool: ^LazyPool,
	count: int,
	chunk_size: int,
	callback: proc(i: int),
	allocator := context.allocator,
) {
	element_ctx := ElementForCtx {
		per_index_callback = callback,
	}
	schedule_parallel_chunks(
		pool,
		count,
		chunk_size,
		&element_ctx,
		element_chunk_runner,
		allocator,
	)
}

ElementForDataCtx :: struct($D: typeid) {
	per_index_with_data: proc(i: int, data: ^D),
	data:                ^D,
}

element_data_chunk_runner :: proc(ctx: ^ElementForDataCtx($D), start: int, end: int) {
	for index in start ..< end {
		ctx.per_index_with_data(index, ctx.data)
	}
}

parallel_for_data :: proc(
	pool: ^LazyPool,
	count: int,
	chunk_size: int,
	callback: proc(i: int, data: ^$D),
	data: ^D,
	allocator := context.allocator,
) {
	element_ctx := ElementForDataCtx(D) {
		per_index_with_data = callback,
		data                = data,
	}
	run_chunk := proc(ctx: ^ElementForDataCtx(D), start: int, end: int) {
		for index in start ..< end {
			ctx.per_index_with_data(index, ctx.data)
		}
	}
	schedule_parallel_chunks(pool, count, chunk_size, &element_ctx, run_chunk, allocator)
}

ChunkForDataCtx :: struct($D: typeid) {
	chunk_callback: proc(start: int, end: int, data: ^D),
	data:           ^D,
}

chunk_data_runner :: proc(ctx: ^ChunkForDataCtx($D), start: int, end: int) {
	ctx.chunk_callback(start, end, ctx.data)
}

parallel_for_chunk :: proc(
	pool: ^LazyPool,
	count: int,
	chunk_size: int,
	callback: proc(start: int, end: int, data: ^$D),
	data: ^D,
	allocator := context.allocator,
) {
	chunk_ctx := ChunkForDataCtx(D) {
		chunk_callback = callback,
		data           = data,
	}
	run_chunk := proc(ctx: ^ChunkForDataCtx(D), start: int, end: int) {
		ctx.chunk_callback(start, end, ctx.data)
	}
	schedule_parallel_chunks(pool, count, chunk_size, &chunk_ctx, run_chunk, allocator)
}

ForEachCtx :: struct($T: typeid) {
	items:    []T,
	callback: proc(index: int, item: ^T),
}

parallel_for_each :: proc(
	pool: ^LazyPool,
	items: []$T,
	chunk_size: int,
	callback: proc(index: int, item: ^T),
	allocator := context.allocator,
) {
	ctx := ForEachCtx(T) {
		items    = items,
		callback = callback,
	}
	run_chunk := proc(c: ^ForEachCtx(T), start: int, end: int) {
		for index in start ..< end {
			c.callback(index, &c.items[index])
		}
	}
	schedule_parallel_chunks(pool, len(items), max(1, chunk_size), &ctx, run_chunk, allocator)
}

parallel_for :: proc {
	parallel_for_simple,
	parallel_for_data,
	parallel_for_chunk,
	parallel_for_each,
}
