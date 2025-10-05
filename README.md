# odin-trinity

Multithreading experiments and utilities.

## lazy_pool

Work-stealing job scheduler/thread pool.

```odin
pool := new(LazyPool)
pool_init(pool, workers)
pool_start(pool)

WorkerArgs :: struct {...}
worker :: proc(args: WorkerArgs) {...}
pool_submit(pool, make_job(worker, args))
```

Better performance than [core:thread.Pool](https://pkg.odin-lang.org/core/thread/#Pool) if:

- you want to run many small jobs, mostly taking <100 microseconds
  - (to take advantage of lock-free data structures)
- your jobs produce more jobs
  - (to take advantage of work stealing)

You can get improved multithreading performance like this (`-o:speed`):

![Throughput comparison](./tps.png)

However if you run long-running jobs or few jobs, lazy_pool performs about the same as (or worse than) the core thread pool.

Implementation based on https://ieeexplore.ieee.org/document/9359172

## lazy_pool/parallel_for

Run `for` loops chunked and multithreaded using the lazy_pool.

```odin
// simple
parallel_for(pool, n, chunk_size, proc(i: int) {})

// slice
slice := []int{1, 2, 3, ...}
parallel_for(pool, slice, 8192, proc(i: int, p: ^int) {})

// data
data: Data = {...}
parallel_for(pool, 1_000_000, 8192, proc(i: int, data: Data), data)

// one call per chunk
parallel_for(pool, 1_000_000, 8192, proc(start: int, end: int, data: Data), data)
```

![parallel_for speed-up](./parallel_for.png)

## deque

Fixed-size lock-free Chase-Lev deque.

## mpsc

Fixed-size lock-free MPSC queue.

## mpmc

Fixed-size lock-free MPMC queue.

## notifier

Notifications with bounded spin loop before sleeping.

## game_of_life

Game of Life, comparing `parallel_for` with regular `for`.
Uses `parallel_for` to divide row updates across multiple workers.
