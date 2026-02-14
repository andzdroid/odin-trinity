package notifier

import "core:sync"

SPIN_COUNT :: 128

load :: sync.atomic_load_explicit
add :: sync.atomic_add_explicit

Notifier :: struct #align (64) {
	epoch: sync.Futex,
}

prepare_wait :: proc "contextless" (n: ^Notifier) -> u32 {
	return u32(load(&n.epoch, .Acquire))
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
}

notify_one :: proc "contextless" (n: ^Notifier) {
	add(&n.epoch, 1, .Release)
	sync.futex_signal(&n.epoch)
}

notify_all :: proc "contextless" (n: ^Notifier) {
	add(&n.epoch, 1, .Release)
	sync.futex_broadcast(&n.epoch)
}
