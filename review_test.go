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
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

func TestRecoveryClassificationUsesErrorIdentity(t *testing.T) {
	for _, test := range []struct {
		err  error
		code string
		exit int
	}{
		{errAuditFailed, "audit_failed", 7},
		{errObservationTimeout, "observation_timeout", 6},
		{errNoProgress, "no_progress", 6},
		{errBudgetExhausted, "budget_exhausted", 6},
		{errInvalidArguments, "invalid_arguments", 2},
		{errors.New("no progress observed"), "query_failed", 3},
	} {
		got := problem(fmt.Errorf("changed contextual wording: %w", test.err), "")
		if got.Code != test.code || got.Exit != test.exit || got.OutcomeUnknown {
			t.Fatalf("%v: %+v", test.err, got)
		}
	}
}

func TestClassifyAdminError(t *testing.T) {
	busy := "Manual compaction is already running for region 47244640257(11, 1)."
	for _, test := range []struct {
		name, message string
		number        uint16
		regions       int
		want          string
	}{
		{"display", busy, 1815, 1, "admin_busy"},
		{"numeric", "Manual compaction is already running for region 47244640257.", 1815, 1, "admin_busy"},
		{"partial failure", busy, 1815, 2, "admin_unknown"},
		{"missing region information", busy, 1815, 0, "admin_unknown"},
		{"different server error", "storage failed", 1815, 1, "admin_unknown"},
		{"different number", busy, 1045, 1, "admin_unknown"},
	} {
		t.Run(test.name, func(t *testing.T) {
			cause := &mysql.MySQLError{Number: test.number, Message: test.message}
			err := classifyAdminError(fmt.Errorf("wrapped: %w", cause), test.regions)
			var admin *AdminError
			if !errors.As(err, &admin) || admin.Kind != test.want || !errors.Is(err, cause) {
				t.Fatalf("error=%v", err)
			}
			failure := problem(err, "")
			known := test.want == "admin_busy"
			if failure.Retryable != known || failure.OutcomeUnknown == known {
				t.Fatalf("failure=%+v", failure)
			}
		})
	}
	if classifyAdminError(nil, 1) != nil {
		t.Fatal("classified success as failure")
	}
	failure := problem(classifyAdminError(context.DeadlineExceeded, 1), "")
	if failure.Code != "admin_unknown" || failure.Exit != 5 || !failure.OutcomeUnknown {
		t.Fatalf("timeout=%+v", failure)
	}
}

func TestInvalidPlanInputsFailBeforeConnection(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"--start", "bad", "--end", "2026-01-01T00:00:00Z"},
		{"--start", "2026-02-01T00:00:00Z", "--end", "2026-01-01T00:00:00Z"},
		{"--start", "1700-01-01T00:00:00Z", "--end", "2200-01-01T00:00:00Z"},
	} {
		// This endpoint cannot connect: an attempted connection would return exit 3.
		args = append([]string{"plan", "--host", "invalid:host", "--table", "events"}, args...)
		var output bytes.Buffer
		if code := command(args, &output); code != 2 {
			t.Fatalf("%v: code=%d %s", args, code, output.String())
		}
	}
	start := time.Now()
	_, err := Scan(context.Background(), &fakeBackend{}, 1, start, start, ScanConfig{}, func(Window) {})
	if !errors.Is(err, errInvalidArguments) {
		t.Fatalf("range error=%v", err)
	}
}

func TestRunConnectionFailureLeavesNoCheckpoint(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	plan := testPlan(t)
	plan.Options.Host, plan.Options.Port = "127.0.0.1", port
	path := filepath.Join(t.TempDir(), "plan.json")
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveJSON(path, plan, true); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		var output bytes.Buffer
		if code := command([]string{"run", "--plan", path, "--state", statePath, "--execute"}, &output); code != 3 {
			t.Fatalf("code=%d %s", code, output.String())
		}
		if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected checkpoint: %v", err)
		}
	}
}

func TestPlanDecodeErrorsAndVersions(t *testing.T) {
	for _, test := range []struct{ body, message string }{
		{`{"version":2,"new_field":true}`, "unsupported schema version 2"},
		{`{"version":1,"options":{"MinWindow":"1m"}}`, "cannot unmarshal"},
		{`{"version":1,"new_field":true}`, "unknown field"},
		{`{"version":1}{"version":1}`, "invalid JSON"},
		{`{"version":1,"plan":{"version":2,"new_field":true}}`, "unsupported schema version 2"},
	} {
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		statePath := filepath.Join(t.TempDir(), "state.json")
		code := command([]string{"run", "--plan", path, "--state", statePath, "--execute"}, &output)
		if code != 2 || !bytes.Contains(output.Bytes(), []byte(test.message)) {
			t.Fatalf("code=%d %s", code, output.String())
		}
		if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("unexpected checkpoint: %v", err)
		}
	}
}

func TestCommandSpecificFlags(t *testing.T) {
	for _, args := range [][]string{
		{"run", "--plan", "plan.json", "--state", "state.json"},
		{"run", "--execute", "--ack-unknown"},
		{"plan", "--execute"},
		{"plan", "--state", "state.json"},
		{"resume", "--execute", "--plan", "plan.json"},
		{"diagnose", "--start", "2026-01-01T00:00:00Z"},
	} {
		var output bytes.Buffer
		if code := command(args, &output); code != 2 {
			t.Fatalf("%v: code=%d %s", args, code, output.String())
		}
	}
	if got := literal(`a\b'c`); got != `'a\\b''c'` {
		t.Fatalf("literal=%q", got)
	}
}

func TestCommandSpecificConfigKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte("state = \"unexpected.json\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if code := command([]string{"tables", "--config", path}, &output); code != 2 {
		t.Fatalf("code=%d %s", code, output.String())
	}
}
