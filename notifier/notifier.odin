package notifier

import "core:sync"

SPIN_COUNT :: 128

load :: sync.atomic_load_explicit
store :: sync.atomic_store_explicit
add :: sync.atomic_add_explicit
sub :: sync.atomic_sub_explicit

Notifier :: struct #align (64) {
	epoch:   sync.Futex,
	waiters: u32,
}

prepare_wait :: proc "contextless" (n: ^Notifier) -> u32 {
	add(&n.waiters, 1, .Relaxed)
	return u32(load(&n.epoch, .Acquire))
}

cancel_wait :: proc "contextless" (n: ^Notifier) {
	sub(&n.waiters, 1, .Relaxed)
}

commit_wait :: proc "contextless" (n: ^Notifier, old: u32) {
	// bounded spin loop
	for i in 0 ..< SPIN_COUNT {
		if u32(load(&n.epoch, .Acquire)) != old {
			break
		}
		sync.cpu_relax()
	}

	// sleep
	for u32(load(&n.epoch, .Acquire)) == old {
		sync.futex_wait(&n.epoch, old)
	}

	sub(&n.waiters, 1, .Relaxed)
}

notify_one :: proc "contextless" (n: ^Notifier) {
	if load(&n.waiters, .Relaxed) == 0 {
		return
	}
	add(&n.epoch, 1, .Release)
	sync.futex_signal(&n.epoch)
}

notify_all :: proc "contextless" (n: ^Notifier) {
	waiters := load(&n.waiters, .Acquire)
	if waiters == 0 {
		return
	}
	add(&n.epoch, 1, .Release)
	for _ in 0 ..< int(waiters) {
		sync.futex_signal(&n.epoch)
	}
}
