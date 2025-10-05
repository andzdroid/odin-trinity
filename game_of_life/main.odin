package game_of_life

import lp "../lazy_pool"
import "core:math/rand"
import "core:time"
import rl "vendor:raylib"

WINDOW_WIDTH :: 1280
WINDOW_HEIGHT :: 720
CELL_SIZE :: 1
GRID_WIDTH :: WINDOW_WIDTH / CELL_SIZE
GRID_HEIGHT :: WINDOW_HEIGHT / CELL_SIZE
CHUNK_ROWS :: 4
CHUNKS :: (GRID_HEIGHT + CHUNK_ROWS - 1) / CHUNK_ROWS

GameState :: struct {
	current: []bool,
	next:    []bool,
}

ComputeCtx :: struct {
	read:  []bool,
	write: []bool,
}

index_of :: proc(x: int, y: int) -> int {
	return y * GRID_WIDTH + x
}

neighbours :: proc(ctx: ^ComputeCtx, x: int, y: int) -> int {
	left := (x - 1 + GRID_WIDTH) % GRID_WIDTH
	right := (x + 1) % GRID_WIDTH
	up := (y - 1 + GRID_HEIGHT) % GRID_HEIGHT
	down := (y + 1) % GRID_HEIGHT
	count := 0
	if ctx.read[index_of(left, y)] {count += 1}
	if ctx.read[index_of(right, y)] {count += 1}
	if ctx.read[index_of(x, up)] {count += 1}
	if ctx.read[index_of(x, down)] {count += 1}
	if ctx.read[index_of(left, up)] {count += 1}
	if ctx.read[index_of(right, up)] {count += 1}
	if ctx.read[index_of(left, down)] {count += 1}
	if ctx.read[index_of(right, down)] {count += 1}
	return count
}

compute_rows :: proc(start: int, end: int, data: ^ComputeCtx) {
	for y in start ..< end {
		for x in 0 ..< GRID_WIDTH {
			idx := index_of(x, y)
			alive := data.read[idx]
			neighbors := neighbours(data, x, y)
			new_alive :=
				(alive && (neighbors == 2 || neighbors == 3)) || (!alive && neighbors == 3)
			data.write[idx] = new_alive
		}
	}
}

randomise :: proc(gs: ^GameState) {
	for i in 0 ..< len(gs.current) {
		gs.current[i] = (rand.uint32() & 1) == 1
	}
}

clear :: proc(gs: ^GameState) {
	for i in 0 ..< len(gs.current) {
		gs.current[i] = false
	}
}

swap_grids :: proc(gs: ^GameState) {
	gs.current, gs.next = gs.next, gs.current
}

step_parallel :: proc(gs: ^GameState, pool: ^lp.LazyPool) {
	ctx := ComputeCtx{gs.current, gs.next}
	lp.parallel_for(pool, GRID_HEIGHT, CHUNK_ROWS, compute_rows, &ctx)
	swap_grids(gs)
}

step_serial :: proc(gs: ^GameState) {
	ctx := ComputeCtx{gs.current, gs.next}
	compute_rows(0, GRID_HEIGHT, &ctx)
	swap_grids(gs)
}

draw_world :: proc(gs: ^GameState, cell_size: int) {
	for y in 0 ..< GRID_HEIGHT {
		for x in 0 ..< GRID_WIDTH {
			if gs.current[index_of(x, y)] {
				rl.DrawRectangle(
					i32(x * cell_size),
					i32(y * cell_size),
					i32(cell_size),
					i32(cell_size),
					rl.BEIGE,
				)
			}
		}
	}
}

toggle :: proc(gs: ^GameState, x: int, y: int) {
	if x < 0 || y < 0 || x >= GRID_WIDTH || y >= GRID_HEIGHT {
		return
	}
	idx := index_of(x, y)
	gs.current[idx] = !gs.current[idx]
}

main :: proc() {
	rl.InitWindow(i32(WINDOW_WIDTH), i32(WINDOW_HEIGHT), "Game of Life")
	rl.SetTargetFPS(60)
	defer rl.CloseWindow()

	state := GameState {
		current = make([]bool, GRID_WIDTH * GRID_HEIGHT),
		next    = make([]bool, GRID_WIDTH * GRID_HEIGHT),
	}
	defer delete(state.current)
	defer delete(state.next)

	randomise(&state)

	pool := new(lp.LazyPool)
	lp.pool_init(pool, 10)
	lp.pool_start(pool)
	defer lp.pool_destroy(pool)
	defer lp.pool_stop(pool)

	running := true
	use_parallel := true
	step_ms: time.Duration = 0
	cells_per_sec: f64 = 0
	chunks_per_sec: f64 = 0
	last_display_update: time.Time = time.now()

	for !rl.WindowShouldClose() {
		if rl.IsKeyPressed(.SPACE) {
			running = !running
		}
		if rl.IsKeyPressed(.P) {
			use_parallel = !use_parallel
		}
		if rl.IsKeyPressed(.R) {
			randomise(&state)
		}
		if rl.IsKeyPressed(.C) {
			clear(&state)
		}

		if rl.IsMouseButtonPressed(.LEFT) {
			pos := rl.GetMousePosition()
			x := int(pos.x) / CELL_SIZE
			y := int(pos.y) / CELL_SIZE
			toggle(&state, x, y)
		}

		if running {
			start := time.now()
			if use_parallel {
				step_parallel(&state, pool)
			} else {
				step_serial(&state)
			}
			elapsed_ms := time.since(start)

			if time.since(last_display_update) >= time.Duration(500 * time.Millisecond) {
				now := time.now()
				last_display_update = now
				cells := GRID_WIDTH * GRID_HEIGHT
				num_chunks := use_parallel ? CHUNKS : 1
				step_ms = elapsed_ms
				cells_per_sec = f64(cells) / time.duration_seconds(elapsed_ms)
				chunks_per_sec = f64(num_chunks) / time.duration_seconds(elapsed_ms)
			}
		}

		rl.BeginDrawing()
		rl.ClearBackground(rl.DARKGRAY)
		draw_world(&state, CELL_SIZE)
		rl.DrawText(
			"SPACE: Pause/Run  R: Randomise  C: Clear  P: Mode  LMB: Toggle Cell",
			10,
			10,
			18,
			rl.WHITE,
		)
		if running {
			rl.DrawText("Running", 10, 34, 18, rl.GREEN)
		} else {
			rl.DrawText("Paused", 10, 34, 18, rl.RED)
		}
		mode_text: cstring = use_parallel ? "Mode: Parallel" : "Mode: Serial"
		rl.DrawText(mode_text, 10, 58, 18, rl.WHITE)
		rl.DrawText(rl.TextFormat("Step: %.2f ms", step_ms), 10, 82, 18, rl.WHITE)
		rl.DrawText(rl.TextFormat("Cells/s: %s", abbreviate(cells_per_sec)), 10, 106, 18, rl.WHITE)
		rl.DrawText(
			rl.TextFormat("Chunks/s: %s", abbreviate(chunks_per_sec)),
			10,
			130,
			18,
			rl.WHITE,
		)
		rl.EndDrawing()
	}
}

abbreviate :: proc(v: f64) -> cstring {
	if v >= 1_000_000_000.0 {
		return rl.TextFormat("%.1fB", v / 1_000_000_000.0)
	}
	if v >= 1_000_000.0 {
		return rl.TextFormat("%.1fM", v / 1_000_000.0)
	}
	if v >= 1_000.0 {
		return rl.TextFormat("%.1fk", v / 1_000.0)
	}
	return rl.TextFormat("%.0f", v)
}
