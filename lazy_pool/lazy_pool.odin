package lazy_pool

import "../deque"
import "../mpmc"
import "../notifier"
import "core:sync"
import "core:thread"

SPIN_TRIES :: 8
MAX_JOBS_PER_WORKER :: 65536

@(thread_local)
start_index: u32

@(thread_local)
current_worker: ^Worker

load :: sync.atomic_load_explicit
store :: sync.atomic_store_explicit
add :: sync.atomic_add_explicit

WorkerDeque :: deque.Deque(Job, MAX_JOBS_PER_WORKER)
GlobalQueue :: mpmc.Queue(Job, MAX_JOBS_PER_WORKER)

Job :: struct {
	run:   proc(ctx: rawptr, data: rawptr),
	ctx:   rawptr,
	data:  rawptr,
	group: ^JobGroup,
}

make_job :: proc(fn: proc(_: ^$T), data: ^T, group: ^JobGroup = nil) -> Job {
	thunk := proc(ctx: rawptr, p: rawptr) {
		(cast(proc(_: ^T))ctx)(cast(^T)p)
	}
	return Job{run = thunk, ctx = rawptr(fn), data = data, group = group}
}

JobGroup :: struct {
	pool:    ^LazyPool,
	wake:    notifier.Notifier,
	using _: struct #align (64) {
		pending: i64,
	},
}

Worker :: struct {
	id:   int,
	pool: ^LazyPool,
}

LazyPool :: struct {
	using _:     struct #align (64) {
		num_actives: i64,
	},
	using _:     struct #align (64) {
		num_thieves: i64,
	},
	using _:     struct #align (64) {
		pending_jobs: i64,
	},
	using _:     struct #align (64) {
		finishing: bool,
	},
	using _:     struct #align (64) {
		global_wakeup: notifier.Notifier,
	},
	using _:     struct #align (64) {
		running: bool,
	},
	workers:     []Worker,
	deques:      []WorkerDeque,
	threads:     []^thread.Thread,
	tasks:       GlobalQueue,
	steal_bound: int,
	yield_bound: int,
}

pool_init :: proc(pool: ^LazyPool, num_workers: int, allocator := context.allocator) {
	assert(num_workers > 0, "lazy_pool.pool_init requires at least one worker")

	mpmc.mpmc_init(&pool.tasks)

	pool.workers = make([]Worker, num_workers, allocator)
	pool.deques = make([]WorkerDeque, num_workers, allocator)
	pool.threads = make([]^thread.Thread, num_workers, allocator)
	pool.running = true
	pool.finishing = false
	pool.num_actives = 0
	pool.num_thieves = 0
	pool.pending_jobs = 0
	pool.steal_bound = 2 * (num_workers + 1)
	pool.yield_bound = 100

	for i in 0 ..< num_workers {
		pool.workers[i] = Worker {
			id   = i,
			pool = pool,
		}

		thread := thread.create(worker_loop)
		thread.data = &pool.workers[i]
		thread.user_index = i
		pool.threads[i] = thread
	}
}

pool_destroy :: proc(pool: ^LazyPool, allocator := context.allocator) {
	delete(pool.workers, allocator)
	delete(pool.deques, allocator)
	delete(pool.threads, allocator)
}

spawn :: proc {
	pool_submit,
	pool_submit_fn,
	worker_submit,
	worker_submit_fn,
	group_submit,
	group_submit_fn,
}

// Enqueue a job onto the global job queue.
pool_submit :: proc(pool: ^LazyPool, j: Job) -> bool {
	if current_worker != nil && current_worker.pool == pool {
		return worker_submit(j)
	}

	if j.group != nil {
		assert(
			j.group.pool == pool,
			"lazy_pool.pool_submit requires the job group to belong to the submitted pool",
		)
	}

	if !begin_submit(pool, false) {
		return false
	}
	if j.group != nil {
		add(&j.group.pending, 1, .Relaxed)
	}
	ok := mpmc.mpmc_enqueue(&pool.tasks, j)
	if ok {
		notifier.notify_one(&pool.global_wakeup)
	} else {
		cancel_submit(pool, j)
	}
	return ok
}

pool_submit_fn :: proc(pool: ^LazyPool, fn: proc(_: ^$T), data: ^T) -> bool {
	j := make_job(fn, data)
	return pool_submit(pool, j)
}

// Enqueue a job onto the current worker's local job queue.
// Faster than pool_submit.
// Returns false if not called from a worker thread.
worker_submit :: proc(j: Job) -> bool {
	if current_worker == nil {
		return false
	}
	if j.group != nil {
		assert(
			j.group.pool == current_worker.pool,
			"lazy_pool.worker_submit requires the job group to belong to the current worker pool",
		)
	}

	pool := current_worker.pool
	if !begin_submit(pool, true) {
		return false
	}
	if j.group != nil {
		add(&j.group.pending, 1, .Relaxed)
	}
	ok := deque.deque_push(&pool.deques[current_worker.id], j)
	if !ok {
		cancel_submit(pool, j)
	}
	return ok
}

worker_submit_fn :: proc(fn: proc(_: ^$T), data: ^T) -> bool {
	j := make_job(fn, data)
	return worker_submit(j)
}

// Enqueue a job as part of a job group.
// Will automatically pick between pool_submit and worker_submit.
group_submit :: proc(group: ^JobGroup, j: Job) -> bool {
	assert(group != nil, "lazy_pool.group_submit requires a group")
	assert(group.pool != nil, "lazy_pool.group_submit requires an initialized group")
	j := j
	j.group = group
	return pool_submit(group.pool, j)
}

group_submit_fn :: proc(group: ^JobGroup, fn: proc(_: ^$T), data: ^T) -> bool {
	j := make_job(fn, data)
	return group_submit(group, j)
}

@(private = "file")
worker_loop :: proc(thread: ^thread.Thread) {
	worker := cast(^Worker)thread.data
	current_worker = worker
	pool := worker.pool
	current_job: Job
	has_job := false

	for load(&pool.running, .Acquire) {
		exploit_task(&current_job, &has_job, worker)
		if !wait_for_task(&current_job, &has_job, worker) {
			break
		}
	}

	current_worker = nil
}

@(private = "file")
begin_submit :: proc(pool: ^LazyPool, allow_while_finishing: bool) -> bool {
	add(&pool.pending_jobs, 1, .Seq_Cst)
	if !load(&pool.running, .Seq_Cst) ||
	   (!allow_while_finishing && load(&pool.finishing, .Seq_Cst)) {
		add(&pool.pending_jobs, -1, .Seq_Cst)
		return false
	}
	return true
}

@(private = "file")
cancel_submit :: proc(pool: ^LazyPool, job: Job) {
	if job.group != nil {
		add(&job.group.pending, -1, .Relaxed)
	}
	add(&pool.pending_jobs, -1, .Seq_Cst)
}

@(private = "file")
finish_job :: proc(pool: ^LazyPool, job: ^Job) {
	if job.group != nil {
		if add(&job.group.pending, -1, .Release) == 1 {
			notifier.notify_all(&job.group.wake)
		}
	}
	add(&pool.pending_jobs, -1, .Seq_Cst)
}

@(private = "file")
exploit_task :: proc(current_job: ^Job, has_job: ^bool, worker: ^Worker) {
	pool := worker.pool
	worker_deque := &pool.deques[worker.id]

	if !has_job^ {
		if job_value, ok := deque.deque_pop(worker_deque); ok {
			current_job^ = job_value
			has_job^ = true
		} else {
			return
		}
	}

	previous_active_count := add(&pool.num_actives, 1, .Acq_Rel)
	if previous_active_count == 0 && load(&pool.num_thieves, .Acquire) == 0 {
		notifier.notify_one(&pool.global_wakeup)
	}

	for {
		current_job.run(current_job.ctx, current_job.data)
		finish_job(pool, current_job)
		if next_job, ok2 := deque.deque_pop(worker_deque); ok2 {
			current_job^ = next_job
			has_job^ = true
			continue
		}
		has_job^ = false
		break
	}

	add(&pool.num_actives, -1, .Acq_Rel)
}

@(private = "file")
explore_task :: proc(current_job: ^Job, worker: ^Worker) -> bool {
	pool := worker.pool
	failed_steal_attempts := 0
	yield_attempts := 0
	total_workers := len(pool.workers)

	for load(&pool.running, .Acquire) {
		// prefer the global queue first
		if job_value, ok := mpmc.mpmc_dequeue(&pool.tasks); ok {
			current_job^ = job_value
			return true
		}

		// attempt to steal from other workers
		if total_workers > 1 {
			for _ in 0 ..< min(SPIN_TRIES, total_workers - 1) {
				victim_index := int(start_index) % total_workers
				start_index += 1
				if victim_index == worker.id {
					victim_index = (victim_index + 1) % total_workers
				}
				victim_deque := &pool.deques[victim_index]
				if job_value, ok := deque.deque_steal(victim_deque); ok {
					current_job^ = job_value
					return true
				}
				sync.cpu_relax()
			}
		}

		failed_steal_attempts += 1
		if failed_steal_attempts >= pool.steal_bound {
			thread.yield()
			yield_attempts += 1
			failed_steal_attempts = 0
			if yield_attempts >= pool.yield_bound {
				break
			}
		}
	}

	return false
}

@(private = "file")
wait_for_task :: proc(current_job: ^Job, has_job: ^bool, worker: ^Worker) -> bool {
	pool := worker.pool

	add(&pool.num_thieves, 1, .Acq_Rel)

	if explore_task(current_job, worker) {
		has_job^ = true
		if add(&pool.num_thieves, -1, .Acq_Rel) == 1 {
			notifier.notify_one(&pool.global_wakeup)
		}
		return true
	}

	if !mpmc.mpmc_is_empty(&pool.tasks) {
		if job_value, ok := mpmc.mpmc_dequeue(&pool.tasks); ok {
			current_job^ = job_value
			has_job^ = true
			if add(&pool.num_thieves, -1, .Acq_Rel) == 1 {
				notifier.notify_one(&pool.global_wakeup)
			}
			return true
		}
	}

	old_epoch := notifier.prepare_wait(&pool.global_wakeup)

	if !mpmc.mpmc_is_empty(&pool.tasks) {
		if job_value, ok := mpmc.mpmc_dequeue(&pool.tasks); ok {
			current_job^ = job_value
			has_job^ = true
			if add(&pool.num_thieves, -1, .Acq_Rel) == 1 {
				notifier.notify_one(&pool.global_wakeup)
			}
			return true
		}
	}

	if !load(&pool.running, .Acquire) {
		notifier.notify_all(&pool.global_wakeup)
		add(&pool.num_thieves, -1, .Acq_Rel)
		return false
	}

	if add(&pool.num_thieves, -1, .Acq_Rel) == 1 && load(&pool.num_actives, .Acquire) > 0 {
		return true
	}

	notifier.commit_wait(&pool.global_wakeup, old_epoch)
	return true
}

group_wait :: proc(g: ^JobGroup) {
	for {
		if load(&g.pending, .Acquire) == 0 {
			break
		}
		epoch := notifier.prepare_wait(&g.wake)
		if load(&g.pending, .Acquire) == 0 {
			break
		}
		notifier.commit_wait(&g.wake, epoch)
	}
}

pool_start :: proc(pool: ^LazyPool) {
	for t in pool.threads {
		thread.start(t)
	}
}

pool_stop :: proc(pool: ^LazyPool) {
	store(&pool.running, false, .Seq_Cst)

	notifier.notify_all(&pool.global_wakeup)

	for i in 0 ..< len(pool.threads) {
		t := pool.threads[i]
		if t != nil {
			thread.join(t)
			thread.destroy(t)
			pool.threads[i] = nil
		}
	}
}

pool_finish :: proc(pool: ^LazyPool) {
	assert(
		current_worker == nil || current_worker.pool != pool,
		"lazy_pool.pool_finish cannot be called from one of its workers",
	)

	store(&pool.finishing, true, .Seq_Cst)
	for load(&pool.pending_jobs, .Seq_Cst) > 0 {
		notifier.notify_all(&pool.global_wakeup)
		thread.yield()
	}

	pool_stop(pool)
}
