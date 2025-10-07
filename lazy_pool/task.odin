package lazy_pool

import "core:time"

TaskId :: distinct int

@(private = "file")
TaskCtx :: struct($N, $D: int) {
	tasks: ^TasksConfig(N, D),
	id:    TaskId,
}

TaskNode :: struct($N, $D: int) {
	using _:            struct #align (64) {
		pending: i32,
	},
	job:                Job,
	dependencies:       [D]TaskId,
	dependencies_count: int,
	ctx:                TaskCtx(N, D),
}

TasksConfig :: struct($N, $D: int) {
	nodes:      [N]TaskNode(N, D),
	node_count: int,
	group:      JobGroup,
}

tasks_add :: proc {
	tasks_add_job,
	tasks_add_fn,
}

tasks_add_job :: proc(tasks: ^TasksConfig($N, $D), job: Job, deps: ..TaskId) -> TaskId {
	id := tasks.node_count
	tasks.node_count += 1
	tasks.nodes[id] = TaskNode(N, D) {
		job = job,
		ctx = {tasks = tasks, id = TaskId(id)},
	}
	for dep in deps {
		after(tasks, dep, TaskId(id))
	}
	return TaskId(id)
}

tasks_add_fn :: proc(
	tasks: ^TasksConfig($N, $D),
	fn: proc(_: ^$T),
	ctx: ^T,
	deps: ..TaskId,
) -> TaskId {
	job := make_job(fn, ctx)
	return tasks_add_job(tasks, job, ..deps)
}

after :: proc(tasks: ^TasksConfig($N, $D), prereq: TaskId, dependent: TaskId) {
	p := int(prereq)
	dependency_count := tasks.nodes[p].dependencies_count
	tasks.nodes[p].dependencies_count += 1
	tasks.nodes[p].dependencies[dependency_count] = dependent

	q := int(dependent)
	tasks.nodes[q].pending += 1
}

@(private = "file")
schedule_node :: proc(tasks: ^TasksConfig($N, $D), id: TaskId) {
	node := &tasks.nodes[int(id)]

	run_task :: proc(ctx: ^TaskCtx(N, D)) {
		tasks := ctx.tasks
		i := int(ctx.id)
		node := &tasks.nodes[i]

		node.job.run(node.job.ctx, node.job.data)

		for i in 0 ..< node.dependencies_count {
			dep := node.dependencies[i]
			dep_node := &tasks.nodes[int(dep)]
			if add(&dep_node.pending, -1, .Acq_Rel) == 1 {
				schedule_node(tasks, dep)
			}
		}
	}

	job := make_job(run_task, &node.ctx)
	for !spawn(&tasks.group, job) {
		time.sleep(10 * time.Microsecond)
	}
}

@(private = "file")
tasks_submit :: proc(tasks: ^TasksConfig($N, $D), pool: ^LazyPool) -> bool {
	tasks.group = JobGroup {
		pool = pool,
	}

	if tasks.node_count == 0 {
		return true
	}

	ready := 0
	for i in 0 ..< tasks.node_count {
		if tasks.nodes[i].pending == 0 {
			schedule_node(tasks, TaskId(i))
			ready += 1
		}
	}
	return ready > 0
}

tasks_wait :: proc(tasks: ^TasksConfig($N, $D)) {
	group_wait(&tasks.group)
}

tasks_run :: proc(tasks: ^TasksConfig($N, $D), pool: ^LazyPool) -> bool {
	if !tasks_submit(tasks, pool) {
		return false
	}
	tasks_wait(tasks)
	return true
}
