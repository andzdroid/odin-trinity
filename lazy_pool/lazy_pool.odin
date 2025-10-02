package lazy_pool

import deque "../deque"
import mpmc "../mpmc"
import notifier "../notifier"
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
	run:  proc(ctx: rawptr, data: rawptr),
	ctx:  rawptr,
	data: rawptr,
}

make_job :: proc(fn: proc(_: ^$T), data: ^T) -> Job {
	thunk := proc(ctx: rawptr, p: rawptr) {
		(cast(proc(_: ^T))ctx)(cast(^T)p)
	}
	return Job{run = thunk, ctx = rawptr(fn), data = data}
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
		global_wakeup: notifier.Notifier,
	},
	running:     bool,
	workers:     []Worker,
	deques:      []WorkerDeque,
	threads:     []^thread.Thread,
	tasks:       GlobalQueue,
	steal_bound: int,
	yield_bound: int,
}

pool_init :: proc(pool: ^LazyPool, num_workers: int, allocator := context.allocator) {
	mpmc.mpmc_init(&pool.tasks)

	pool.workers = make([]Worker, num_workers, allocator)
	pool.deques = make([]WorkerDeque, num_workers, allocator)
	pool.threads = make([]^thread.Thread, num_workers, allocator)
	pool.running = true
	pool.num_actives = 0
	pool.num_thieves = 0
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

pool_submit :: proc(pool: ^LazyPool, j: Job) -> bool {
	ok := mpmc.mpmc_enqueue(&pool.tasks, j)
	if ok {
		notifier.notify_one(&pool.global_wakeup)
	}
	return ok
}

// Enqueue a job onto the current worker's local deque.
// Returns false if not called from a worker thread.
worker_submit :: proc(j: Job) -> bool {
	if current_worker == nil {
		return false
	}
	return deque.deque_push(&current_worker.pool.deques[current_worker.id], j)
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
		notifier.cancel_wait(&pool.global_wakeup)
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
		notifier.cancel_wait(&pool.global_wakeup)
		notifier.notify_all(&pool.global_wakeup)
		add(&pool.num_thieves, -1, .Acq_Rel)
		return false
	}

	if add(&pool.num_thieves, -1, .Acq_Rel) == 1 && load(&pool.num_actives, .Acquire) > 0 {
		notifier.cancel_wait(&pool.global_wakeup)
		return true
	}

	notifier.commit_wait(&pool.global_wakeup, old_epoch)
	return true
}

pool_start :: proc(pool: ^LazyPool) {
	for t in pool.threads {
		thread.start(t)
	}
}

pool_stop :: proc(pool: ^LazyPool) {
	store(&pool.running, false, .Release)

	notifier.notify_all(&pool.global_wakeup)

	for t in pool.threads {
		thread.join(t)
		thread.destroy(t)
	}
}

pool_finish :: proc(pool: ^LazyPool) {
	for {
		task := mpmc.mpmc_dequeue(&pool.tasks) or_break
		task.run(task.ctx, task.data)
	}

	store(&pool.running, false, .Release)
}
