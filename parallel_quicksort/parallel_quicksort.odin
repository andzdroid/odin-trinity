package parallel_quicksort

import lp "../lazy_pool"
import "base:runtime"
import "core:time"

Params :: struct {
	pool:      ^lp.LazyPool,
	a:         []int,
	group:     ^lp.JobGroup,
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
	pool: ^lp.LazyPool,
	a: []int,
	group: ^lp.JobGroup,
	allocator: runtime.Allocator,
) {
	params := new(Params, allocator)
	params.pool = pool
	params.a = a
	params.group = group
	params.allocator = allocator
	job := lp.make_job(quicksort_job, params)
	for !lp.spawn(group, job) {
		time.sleep(10 * time.Microsecond)
	}
}

quicksort_job :: proc(p: ^Params) {
	defer free(p, p.allocator)

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

	submit_child(p.pool, left, p.group, p.allocator)
	submit_child(p.pool, right, p.group, p.allocator)
}

parallel_quicksort :: proc(pool: ^lp.LazyPool, a: []int, allocator := context.allocator) {
	group := lp.JobGroup {
		pool = pool,
	}

	root := new(Params, allocator)
	root.pool = pool
	root.a = a
	root.group = &group
	root.allocator = allocator

	lp.spawn(&group, lp.make_job(quicksort_job, root))
	lp.group_wait(&group)
}
