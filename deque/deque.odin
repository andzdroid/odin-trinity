package deque

import "core:sync"

load :: sync.atomic_load_explicit
store :: sync.atomic_store_explicit
cas_strong :: sync.atomic_compare_exchange_strong_explicit
fence :: sync.atomic_thread_fence

// https://www.dre.vanderbilt.edu/~schmidt/PDF/work-stealing-dequeue.pdf
// N must be a power of 2
Deque :: struct($T: typeid, $N: int) where N > 0 && (N & (N - 1)) == 0 {
	using _: struct #align (64) {
		top: i64,
	},
	using _: struct #align (64) {
		bottom: i64,
	},
	buf:     [N]T,
}

deque_push :: proc "contextless" (deque: ^$D/Deque($T, $N), value: T) -> bool {
	bottom := load(&deque.bottom, .Relaxed)
	top := load(&deque.top, .Acquire)
	// full
	if (bottom - top) >= i64(N) {
		return false
	}
	deque.buf[bottom & i64(N - 1)] = value
	store(&deque.bottom, bottom + 1, .Release)
	return true
}

deque_push_many :: proc "contextless" (deque: ^$D/Deque($T, $N), values: []T) -> int {
	if len(values) == 0 {
		return 0
	}
	bottom := load(&deque.bottom, .Relaxed)
	top := load(&deque.top, .Acquire)
	free := i64(N) - (bottom - top)
	if free <= 0 {
		return 0
	}
	n := min(len(values), int(free))
	for i in 0 ..< n {
		deque.buf[(bottom + i64(i)) & i64(N - 1)] = values[i]
	}
	store(&deque.bottom, bottom + i64(n), .Release)
	return n
}

deque_pop :: proc "contextless" (deque: ^$D/Deque($T, $N)) -> (value: T, ok: bool) {
	bottom := load(&deque.bottom, .Relaxed) - 1
	store(&deque.bottom, bottom, .Relaxed)
	fence(.Seq_Cst)
	top := load(&deque.top, .Acquire)

	// empty
	if top > bottom {
		store(&deque.bottom, top, .Relaxed)
		return {}, false
	}

	out := deque.buf[bottom & i64(N - 1)]

	// fast path
	if top < bottom {
		return out, true
	}

	// slow path
	_, success := cas_strong(&deque.top, top, top + 1, .Seq_Cst, .Seq_Cst)
	store(&deque.bottom, top + 1, .Relaxed)
	return out, success
}

deque_steal :: proc "contextless" (deque: ^$D/Deque($T, $N)) -> (value: T, ok: bool) {
	top := load(&deque.top, .Acquire)
	fence(.Seq_Cst)
	bottom := load(&deque.bottom, .Acquire)

	// empty
	if top >= bottom {
		return {}, false
	}

	out := deque.buf[top & i64(N - 1)]
	_, success := cas_strong(&deque.top, top, top + 1, .Seq_Cst, .Relaxed)
	return out, success
}

deque_count :: proc "contextless" (deque: ^$D/Deque($T, $N)) -> i64 {
	top := load(&deque.top, .Acquire)
	bottom := load(&deque.bottom, .Acquire)
	return max(i64(0), bottom - top)
}

deque_is_empty :: proc "contextless" (deque: ^$D/Deque($T, $N)) -> bool {
	top := load(&deque.top, .Acquire)
	bottom := load(&deque.bottom, .Acquire)
	return top >= bottom
}

deque_is_full :: proc "contextless" (deque: ^$D/Deque($T, $N)) -> bool {
	top := load(&deque.top, .Acquire)
	bottom := load(&deque.bottom, .Acquire)
	return (bottom - top) >= i64(N)
}
