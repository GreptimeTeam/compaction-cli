/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"time"
)

// Backend supplies the storage operations required by scanning and execution.
type Backend interface {
	Probe(context.Context, uint64, time.Time, time.Time) (Window, error)
	Regions(context.Context, uint64) ([]Region, error)
	Compact(context.Context, Diagnosis, Window, time.Duration) error
}

// Scan first covers the entire range, then refines dense windows breadth-first.
// Exhausting the budget preserves a complete, explicitly capped frontier.
func Scan(ctx context.Context, backend Backend, id uint64, start, end time.Time, step time.Duration, options Options, emit func(Window)) ([]Window, error) {
	if !end.After(start) {
		return nil, fmt.Errorf("end must be later than start (UTC, end exclusive)")
	}
	if step < options.MinWindow || step <= 0 {
		return nil, fmt.Errorf("request window must be at least %s", options.MinWindow)
	}
	budget := options.ScanBudget
	if budget < 1 {
		return nil, fmt.Errorf("scan-budget must be positive")
	}
	// Widen only the initial coverage pass if the selected range exceeds budget.
	// The actual windows remain visible for review before any ADMIN submission.
	span := end.Sub(start)
	if !start.Add(span).Equal(end) {
		return nil, fmt.Errorf("time range exceeds the supported duration")
	}
	if span/step >= time.Duration(budget) {
		step = span / time.Duration(budget)
		if span%time.Duration(budget) != 0 {
			step++
		}
	}
	queries := 0
	probe := func(a, b time.Time) (Window, error) {
		if queries >= budget {
			return Window{}, fmt.Errorf("scan query budget exhausted")
		}
		if err := ctx.Err(); err != nil {
			return Window{}, err
		}
		if queries > 0 {
			if err := wait(ctx, options.ScanInterval); err != nil {
				return Window{}, err
			}
		}
		queries++
		w, err := backend.Probe(ctx, id, a, b)
		if err == nil {
			emit(w)
		}
		return w, err
	}
	var plan []Window
	for a := start; a.Before(end); {
		b := a.Add(step)
		if b.After(end) {
			b = end
		}
		w, err := probe(a, b)
		if err != nil {
			return nil, err
		}
		plan = append(plan, w)
		a = b
	}
	plan, err := refineWindows(plan, options.MinWindow, budget, &queries, probe)
	if err != nil {
		return nil, err
	}
	for i := range plan {
		if plan[i].Limited {
			plan[i].State = "limited (budget or minimum window)"
		}
	}
	return plan, nil
}

func refineWindows(plan []Window, minWindow time.Duration, budget int, queries *int, probe func(time.Time, time.Time) (Window, error)) ([]Window, error) {
	for {
		split := false
		next := make([]Window, 0, len(plan))
		for _, w := range plan {
			if w.Limited && w.End.Sub(w.Start)/2 >= minWindow && *queries+2 <= budget {
				mid := w.Start.Add(w.End.Sub(w.Start) / 2)
				left, err := probe(w.Start, mid)
				if err != nil {
					return nil, err
				}
				right, err := probe(mid, w.End)
				if err != nil {
					return nil, err
				}
				next = append(next, left, right)
				split = true
			} else {
				next = append(next, w)
			}
		}
		plan = next
		if !split {
			break
		}
	}
	return plan, nil
}

// Progress reports the current window, attempts and optional table observations.
type Progress struct {
	Index, Calls int
	Window       Window
	Regions      []Region
	Message      string
}

// Runner serializes ADMIN submissions and stops on uncertainty or lack of progress.
type Runner struct {
	Store   Backend
	Options Options
	Pause   func() bool
	Emit    func(Progress)
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Run always refreshes a window before deciding whether to skip or submit.
// Stabilization is an observation heuristic, not proof of scheduler idleness.
func (r Runner) Run(ctx context.Context, diagnosis Diagnosis, plan []Window, index, calls int) (int, int, error) {
	for windowIndex := index; windowIndex < len(plan); windowIndex++ {
		for {
			if r.Pause() {
				return windowIndex, calls, nil
			}
			if err := ctx.Err(); err != nil {
				return windowIndex, calls, err
			}
			window, err := r.Store.Probe(ctx, diagnosis.ID, plan[windowIndex].Start, plan[windowIndex].End)
			if err != nil {
				return windowIndex, calls, err
			}
			if !window.Limited && len(window.Files) <= r.Options.Target {
				window.State = "target reached"
				r.Emit(Progress{windowIndex, calls, window, nil, "Window meets target"})
				break
			}
			if calls >= r.Options.MaxCalls {
				return windowIndex, calls, fmt.Errorf("ADMIN budget reached (%d calls); inspect results before starting a new run", calls)
			}
			if r.Pause() {
				return windowIndex, calls, nil
			}
			before := window.Fingerprint()
			calls++
			window.State = "ADMIN in flight"
			r.Emit(Progress{windowIndex, calls, window, nil, "Submitting one ADMIN; pause stops future submissions"})
			if r.Pause() {
				return windowIndex, calls - 1, nil
			}
			if err = r.Store.Compact(ctx, diagnosis, window, r.Options.AdminTimeout); err != nil {
				return windowIndex, calls, err
			}
			var paused bool
			window, paused, err = r.observe(ctx, diagnosis, windowIndex, calls, window)
			if err != nil || paused {
				return windowIndex, calls, err
			}
			if !window.Limited && len(window.Files) <= r.Options.Target {
				window.State = "target reached"
				r.Emit(Progress{windowIndex, calls, window, nil, "Window meets target after observation"})
				break
			}
			if window.Fingerprint() == before {
				return windowIndex, calls, fmt.Errorf("no progress observed in %s .. %s (%s files); stopped submitting. A capped sample cannot prove global inactivity; inspect or rescan", stamp(window.Start), stamp(window.End), window.Count())
			}
		}
	}
	return len(plan), calls, nil
}

// observe waits for unchanged samples; it does not establish scheduler idleness.
func (r Runner) observe(ctx context.Context, d Diagnosis, index, calls int, window Window) (Window, bool, error) {
	began, stable := time.Now(), time.Now()
	previous := ""
	settled := false
	for time.Since(began) < r.Options.Observe {
		if r.Pause() {
			return window, true, nil
		}
		if err := wait(ctx, r.Options.Poll); err != nil {
			return window, false, err
		}
		after, e := r.Store.Probe(ctx, d.ID, window.Start, window.End)
		if e != nil {
			return window, false, e
		}
		regions, e := r.Store.Regions(ctx, d.ID)
		if e != nil {
			return window, false, e
		}
		var n, b uint64
		for _, reg := range regions {
			n += reg.Files
			b += reg.Bytes
		}
		key := fmt.Sprintf("%s/%d/%d", after.Fingerprint(), n, b)
		if key != previous {
			stable = time.Now()
			previous = key
		}
		after.State = "observing"
		r.Emit(Progress{index, calls, after, regions, "Waiting for stable SST observations (background work may extend outside request)"})
		window = after
		if time.Since(stable) >= r.Options.Quiet {
			settled = true
			break
		}
	}
	if !settled {
		return window, false, fmt.Errorf("observation timed out after %s; no further ADMIN submitted. Background work may still be running; inspect and rescan", r.Options.Observe)
	}
	return window, false, nil
}
