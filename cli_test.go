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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testPlan(t *testing.T) Plan {
	t.Helper()
	o, err := parseOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	o.Table = "events"
	start := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	return Plan{Version: 1, Options: o, Diagnosis: Diagnosis{ID: 1024, Table: o.Table}, Windows: []PlannedWindow{{Start: start, End: start.Add(time.Hour)}}}
}
func TestStateLockAndUnknownOutcomeGuard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.json")
	state := State{Version: 1, Plan: testPlan(t), Status: "admin_in_flight", Calls: 1}
	if err := saveJSON(path, state, true); err != nil {
		t.Fatal(err)
	}
	if err := saveJSON(path, state, true); err == nil {
		t.Fatal("overwrote existing state")
	}
	lock, err := lockState(path)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockState(path); err == nil {
		_ = second.Close()
		t.Fatal("accepted concurrent state writer")
	}
	_ = lock.Close()
	var out bytes.Buffer
	if code := command([]string{"resume", "--state", path, "--execute", "-o", "json"}, &out); code != 5 {
		t.Fatalf("code=%d %s", code, out.String())
	}
	if !strings.Contains(out.String(), "acknowledgement_required") {
		t.Fatal(out.String())
	}
	var got State
	if err = readJSON(path, &got); err != nil {
		t.Fatal(err)
	}
	if got.Calls != 1 || got.Status != "admin_in_flight" {
		t.Fatal("guard changed execution state")
	}
}
func TestInvalidRunFlagsDoNotCreateState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "plan.json")
	state := filepath.Join(dir, "state.json")
	if err := saveJSON(path, testPlan(t), true); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if code := command([]string{"run", "--plan", path, "--state", state, "--execute", "--limit", "300"}, &out); code != 2 {
		t.Fatalf("code=%d %s", code, out.String())
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatal("invalid command created state")
	}
}
func TestOutputContractsAndEmbeddedSkill(t *testing.T) {
	for _, format := range []string{"jsonl", "json", "markdown"} {
		var out bytes.Buffer
		p := Printer{Format: format, Out: &out}
		p.Emit("progress", Progress{Calls: 2, Window: Window{Limited: true, Files: []File{{ID: "private-file-id"}}}}, false)
		p.Emit("result", State{Version: 1, Status: "complete", Plan: testPlan(t), Index: 1, Calls: 2}, true)
		if p.Err != nil {
			t.Fatal(p.Err)
		}
		if strings.Contains(out.String(), "private-file-id") {
			t.Fatal("progress leaked detailed file list")
		}
		if format == "json" {
			var v map[string]any
			if err := json.Unmarshal(out.Bytes(), &v); err != nil || v["type"] != "result" {
				t.Fatal(out.String())
			}
		}
		if format == "jsonl" && len(strings.Split(strings.TrimSpace(out.String()), "\n")) != 2 {
			t.Fatal(out.String())
		}
	}
	var out bytes.Buffer
	if command([]string{"skill"}, &out) != 0 || out.String() != agentSkill {
		t.Fatal("embedded skill differs")
	}
}
