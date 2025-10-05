package lazy_pool

import notifier "../notifier"
import "base:runtime"
import "core:sync"
import "core:time"

Params :: struct {
	pool:      ^LazyPool,
	a:         []int,
	remaining: ^i64,
	done:      ^notifier.Notifier,
	allocator: runtime.Allocator,
}

QS_THRESHOLD :: 128

insertion_sort :: proc(a: []int) {
	for i in 1 ..< len(a) {
		v := a[i]
		j := i - 1
		for ; j >= 0 && a[j] > v; j -= 1 {
			a[j + 1] = a[j]
		}
		a[j + 1] = v
	}
}

partition :: proc(a: []int) -> int {
	p := a[len(a) / 2]
	i, j := 0, len(a) - 1
	for {
		for ; a[i] < p; i += 1 {}
		for ; a[j] > p; j -= 1 {}
		if i >= j {return j}
		a[i], a[j] = a[j], a[i]
		i += 1; j -= 1
	}
}

submit_child :: proc(
	pool: ^LazyPool,
	a: []int,
	remaining: ^i64,
	done: ^notifier.Notifier,
	allocator: runtime.Allocator,
) {
	add(remaining, 1, .Acq_Rel)
	params := new(Params, allocator)
	params.pool = pool
	params.a = a
	params.remaining = remaining
	params.done = done
	params.allocator = allocator
	job := make_job(quicksort_job, params)
	if !worker_submit(job) {
		for !pool_submit(pool, job) {
			time.sleep(10 * time.Microsecond)
		}
	}
}

quicksort_job :: proc(p: ^Params) {
	defer finish_job(p)

	a := p.a
	if len(a) <= 1 {
		return
	}
	if len(a) < QS_THRESHOLD {
		insertion_sort(a)
		return
	}
	m := partition(a)
	left := a[:m + 1]
	right := a[m + 1:]

	submit_child(p.pool, left, p.remaining, p.done, p.allocator)
	submit_child(p.pool, right, p.remaining, p.done, p.allocator)
}

finish_job :: proc(p: ^Params) {
	if add(p.remaining, -1, .Acq_Rel) == 1 {
		notifier.notify_all(p.done)
	}
	free(p, p.allocator)
}

parallel_quicksort :: proc(pool: ^LazyPool, a: []int, allocator := context.allocator) {
	remaining: i64 = 1
	done: notifier.Notifier

	root := new(Params, allocator)
	root.pool = pool
	root.a = a
	root.remaining = &remaining
	root.done = &done
	root.allocator = allocator

	pool_submit(pool, make_job(quicksort_job, root))

	for {
		if load(&remaining, .Acquire) == 0 {
			break
		}
		epoch := notifier.prepare_wait(&done)
		if load(&remaining, .Acquire) == 0 {
			notifier.cancel_wait(&done)
			break
		}
		notifier.commit_wait(&done, epoch)
	}
}
