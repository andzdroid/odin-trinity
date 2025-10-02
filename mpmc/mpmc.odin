package mpmc

import "core:sync"

load :: sync.atomic_load_explicit
store :: sync.atomic_store_explicit
cas_strong :: sync.atomic_compare_exchange_strong_explicit
relax :: sync.cpu_relax

Slot :: struct($T: typeid) {
	seq: u64,
	val: T,
}

// N must be a power of 2
Queue :: struct($T: typeid, $N: int) where N > 0 && (N & (N - 1)) == 0 {
	using _: struct #align (64) {
		tail: u64,
	},
	using _: struct #align (64) {
		head: u64,
	},
	slots:   [N]Slot(T),
}

mpmc_init :: proc "contextless" (queue: ^$Q/Queue($T, $N)) {
	for i in 0 ..< N {
		store(&queue.slots[i].seq, cast(u64)i, .Relaxed)
	}
}

mpmc_enqueue :: proc "contextless" (queue: ^$Q/Queue($T, $N), x: T) -> bool {
	for {
		pos := load(&queue.tail, .Relaxed)
		slot := &queue.slots[cast(int)(pos & u64(N - 1))]
		seq := load(&slot.seq, .Acquire)
		diff := cast(i64)(seq) - cast(i64)(pos)

		if diff == 0 {
			_, success := cas_strong(&queue.tail, pos, pos + 1, .Relaxed, .Relaxed)
			if success {
				slot.val = x
				store(&slot.seq, pos + 1, .Release)
				return true
			}
			relax()
		} else if diff < 0 {
			return false // full
		} else {
			relax()
		}
	}
}

mpmc_dequeue :: proc "contextless" (queue: ^$Q/Queue($T, $N)) -> (value: T, ok: bool) {
	for {
		pos := load(&queue.head, .Relaxed)
		slot := &queue.slots[cast(int)(pos & u64(N - 1))]

		seq := load(&slot.seq, .Acquire)
		diff := cast(i64)(seq) - cast(i64)(pos + 1)

		if diff == 0 {
			_, success := cas_strong(&queue.head, pos, pos + 1, .Relaxed, .Relaxed)
			if success {
				v := slot.val
				store(&slot.seq, pos + cast(u64)N, .Release)
				return v, true
			}
			relax()
		} else if diff < 0 {
			return {}, false // empty
		} else {
			relax()
		}
	}
}

mpmc_count :: proc "contextless" (queue: ^$Q/Queue($T, $N)) -> u64 {
	head := load(&queue.head, .Relaxed)
	tail := load(&queue.tail, .Relaxed)
	if tail < head {
		return 0
	}
	return min(u64(N), tail - head)
}

mpmc_is_empty :: proc "contextless" (queue: ^$Q/Queue($T, $N)) -> bool {
	pos := load(&queue.head, .Relaxed)
	slot := &queue.slots[cast(int)(pos & u64(N - 1))]
	seq := load(&slot.seq, .Acquire)
	diff := cast(i64)(seq) - cast(i64)(pos + 1)
	return diff < 0
}

mpmc_is_full :: proc "contextless" (queue: ^$Q/Queue($T, $N)) -> bool {
	pos := load(&queue.tail, .Relaxed)
	slot := &queue.slots[cast(int)(pos & u64(N - 1))]
	seq := load(&slot.seq, .Acquire)
	diff := cast(i64)(seq) - cast(i64)(pos)
	return diff < 0
}
