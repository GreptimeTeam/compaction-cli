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
	"errors"
	"strings"
	"testing"
	"time"
)

type fakeBackend struct {
	probe          func(time.Time, time.Time) Window
	adminErr       error
	calls, queries int
}

func (f *fakeBackend) Probe(_ context.Context, _ uint64, a, b time.Time) (Window, error) {
	f.queries++
	return f.probe(a, b), nil
}
func (f *fakeBackend) Regions(context.Context, uint64) ([]Region, error) {
	return []Region{{Files: 2, Bytes: 20}}, nil
}
func (f *fakeBackend) Compact(context.Context, Diagnosis, Window, time.Duration) error {
	f.calls++
	return f.adminErr
}
func TestScanSplitsDenseWindowsAndPreservesCoverage(t *testing.T) {
	a := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	b := a.Add(time.Hour)
	f := &fakeBackend{probe: func(a, b time.Time) Window { return Window{Start: a, End: b, Limited: b.Sub(a) > 15*time.Minute} }}
	plan, err := Scan(context.Background(), f, 1, a, b, time.Hour, Options{ScanBudget: 128, MinWindow: time.Minute}, func(Window) {})
	if err != nil || len(plan) != 4 {
		t.Fatalf("plan=%v err=%v", plan, err)
	}
	if !plan[0].Start.Equal(a) || !plan[3].End.Equal(b) {
		t.Fatal("coverage changed")
	}
	for i, w := range plan {
		if w.Limited {
			t.Fatal("should be exact")
		}
		if i > 0 && !plan[i-1].End.Equal(w.Start) {
			t.Fatal("gap or overlap")
		}
	}
}
func TestScanBudgetAndInvalidRanges(t *testing.T) {
	a := time.Now()
	f := &fakeBackend{probe: func(a, b time.Time) Window { return Window{Start: a, End: b, Limited: true} }}
	plan, err := Scan(context.Background(), f, 1, a, a.Add(128*time.Hour), time.Hour, Options{ScanBudget: 128, MinWindow: time.Minute}, func(Window) {})
	if err != nil || f.queries != 128 || len(plan) != 128 || !plan[0].Limited || !plan[len(plan)-1].End.Equal(a.Add(128*time.Hour)) {
		t.Fatalf("%v queries=%d", err, f.queries)
	}
	_, err = Scan(context.Background(), f, 1, a, a, time.Hour, Options{ScanBudget: 128, MinWindow: time.Minute}, func(Window) {})
	if err == nil {
		t.Fatal("accepted empty range")
	}
}
func TestRunnerNeverRetriesUnknownAdmin(t *testing.T) {
	a := time.Now()
	w := Window{Start: a, End: a.Add(time.Hour), Files: []File{{ID: "a"}, {ID: "b"}}}
	f := &fakeBackend{probe: func(time.Time, time.Time) Window { return w }, adminErr: errors.New("connection lost")}
	r := Runner{Store: f, Options: Options{Target: 1, MaxCalls: 10}, Pause: func() bool { return false }, Emit: func(Progress) {}}
	_, calls, err := r.Run(context.Background(), Diagnosis{}, []Window{w}, 0, 0)
	if err == nil || calls != 1 || f.calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
func TestRunnerPauseAndResumeRechecksWindow(t *testing.T) {
	a := time.Now()
	w := Window{Start: a, End: a.Add(time.Hour), Files: []File{{ID: "a"}, {ID: "b"}}}
	paused := false
	f := &fakeBackend{probe: func(time.Time, time.Time) Window { return w }}
	r := Runner{Store: f, Options: Options{Target: 1, MaxCalls: 10, Observe: time.Second}, Pause: func() bool { return paused }, Emit: func(Progress) { paused = true }}
	index, calls, err := r.Run(context.Background(), Diagnosis{}, []Window{w}, 0, 0)
	if err != nil || index != 0 || calls != 0 || f.calls != 0 {
		t.Fatalf("index=%d calls=%d err=%v", index, calls, err)
	}
	paused = false
	w.Files = w.Files[:1]
	r.Emit = func(Progress) {}
	index, calls, err = r.Run(context.Background(), Diagnosis{}, []Window{w}, index, calls)
	if err != nil || index != 1 || calls != 0 || f.queries != 2 {
		t.Fatalf("index=%d calls=%d queries=%d err=%v", index, calls, f.queries, err)
	}
}
func TestRunnerStopsOnNoProgress(t *testing.T) {
	a := time.Now()
	w := Window{Start: a, End: a.Add(time.Hour), Files: []File{{ID: "a"}, {ID: "b"}}}
	f := &fakeBackend{probe: func(time.Time, time.Time) Window { return w }}
	r := Runner{Store: f, Options: Options{Target: 1, MaxCalls: 10, Poll: time.Millisecond, Quiet: time.Millisecond, Observe: time.Second}, Pause: func() bool { return false }, Emit: func(Progress) {}}
	_, _, err := r.Run(context.Background(), Diagnosis{}, []Window{w}, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "no progress") || f.calls != 1 {
		t.Fatalf("calls=%d err=%v", f.calls, err)
	}
}
func TestLimitedSampleNeverCountsAsTarget(t *testing.T) {
	a := time.Now()
	w := Window{Start: a, End: a.Add(time.Hour), Limited: true, Files: []File{{ID: "a"}}}
	f := &fakeBackend{probe: func(time.Time, time.Time) Window { return w }, adminErr: errors.New("stop")}
	r := Runner{Store: f, Options: Options{Target: 5, MaxCalls: 1}, Pause: func() bool { return false }, Emit: func(Progress) {}}
	_, _, err := r.Run(context.Background(), Diagnosis{}, []Window{w}, 0, 0)
	if err == nil || f.calls != 1 {
		t.Fatal("capped sample incorrectly skipped")
	}
}
func TestPasswordRedactedAndOptionsRejectUnsafeLimits(t *testing.T) {
	if strings.Contains(friendly(errors.New("secret failed"), "secret"), "secret") {
		t.Fatal("password leaked")
	}
	if (Options{Port: 4002, Limit: 1001}).Validate() == nil {
		t.Fatal("unbounded limit accepted")
	}
	if literal("a'b") != "'a''b'" {
		t.Fatal("SQL literal escaping")
	}
}

func TestBudgetedFrontierAlwaysCoversEntireRange(t *testing.T) {
	a := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	end := a.Add(7 * 24 * time.Hour)
	for _, budget := range []int{1, 2, 7, 64, 128} {
		f := &fakeBackend{probe: func(a, b time.Time) Window {
			return Window{Start: a, End: b, Limited: true, Files: []File{{ID: "sample"}}}
		}}
		plan, err := Scan(context.Background(), f, 1, a, end, time.Hour, Options{ScanBudget: budget, MinWindow: time.Minute}, func(Window) {})
		if err != nil || len(plan) == 0 || f.queries > budget {
			t.Fatalf("budget %d: queries %d err %v", budget, f.queries, err)
		}
		cursor := a
		for _, w := range plan {
			if !w.Start.Equal(cursor) || !w.End.After(w.Start) {
				t.Fatal("coverage gap/overlap")
			}
			cursor = w.End
		}
		if !cursor.Equal(end) {
			t.Fatal("lost tail of time range")
		}
	}
}
func TestObservationTimeoutStopsFurtherSubmissions(t *testing.T) {
	a := time.Now()
	w := Window{Start: a, End: a.Add(time.Hour), Files: []File{{ID: "a"}, {ID: "b"}}}
	f := &fakeBackend{probe: func(time.Time, time.Time) Window { return w }}
	r := Runner{Store: f, Options: Options{Target: 1, MaxCalls: 10, Poll: time.Millisecond, Quiet: time.Second, Observe: 5 * time.Millisecond}, Pause: func() bool { return false }, Emit: func(Progress) {}}
	_, _, err := r.Run(context.Background(), Diagnosis{}, []Window{w}, 0, 0)
	if err == nil || !strings.Contains(err.Error(), "observation timed out") || f.calls != 1 {
		t.Fatalf("calls %d err %v", f.calls, err)
	}
}

func TestScanExtremeRangesRespectBudget(t *testing.T) {
	start := time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		end       time.Time
		budget    int
		wantError bool
	}{
		{"saturated", time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), 1, true},
		{"saturated multiple probes", time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC), 3, true},
		{"maximum duration", start.Add(time.Duration(1<<63 - 1)), 1, false},
		{"maximum duration divided", start.Add(time.Duration(1<<63 - 1)), 3, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := &fakeBackend{probe: func(a, b time.Time) Window {
				if !b.After(a) {
					t.Fatal("scan moved backwards")
				}
				return Window{Start: a, End: b, Limited: true}
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			windows, err := Scan(ctx, backend, 1, start, test.end, time.Hour, Options{ScanBudget: test.budget, MinWindow: time.Second}, func(Window) {})
			if test.wantError {
				if err == nil || backend.queries != 0 {
					t.Fatalf("err=%v queries=%d", err, backend.queries)
				}
				return
			}
			if err != nil || backend.queries > test.budget || len(windows) == 0 {
				t.Fatalf("err=%v queries=%d", err, backend.queries)
			}
			if !windows[0].Start.Equal(start) || !windows[len(windows)-1].End.Equal(test.end) {
				t.Fatal("coverage lost")
			}
		})
	}
}
