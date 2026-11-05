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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigPrecedenceAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.toml")
	for _, tc := range []struct {
		body string
		args []string
		want string
	}{
		{`limit=300
scan-budget=64
scan-interval="500ms"`, []string{"--limit", "400"}, ""},
		{`limti=200`, nil, "unknown config key"},
		{`password="secret"`, nil, "not allowed"},
		{`poll="nonsense"`, nil, "config key"},
		{`limit=5000`, nil, "2..1000"},
		{`scan-budget=0`, nil, "scan-budget"},
		{`min-window="0s"`, nil, "min-window"},
	} {
		if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
			t.Fatal(err)
		}
		args := append([]string{"--config", path}, tc.args...)
		o, err := parseOptions(args)
		if tc.want != "" {
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("%s: %v", tc.body, err)
			}
			continue
		}
		if err != nil || o.Limit != 400 || o.ScanBudget != 64 || o.ScanInterval != 500*time.Millisecond {
			t.Fatalf("%+v %v", o, err)
		}
	}
}

func TestConfigRejectsOversizeInsteadOfTruncating(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.toml")
	if err := os.WriteFile(path, []byte("#"+strings.Repeat("x", 1<<20)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := parseOptions([]string{"--config", path}); err == nil || !strings.Contains(err.Error(), "exceeds 1 MiB") {
		t.Fatalf("error=%v", err)
	}
}

func TestObservationBudgetIncludesInitialPoll(t *testing.T) {
	for _, tc := range []struct {
		budget string
		valid  bool
	}{
		{"2s", false}, {"3s", false}, {"4s", true},
	} {
		_, err := parseOptions([]string{"--poll", "1s", "--quiet", "2s", "--observe-timeout", tc.budget})
		if (err == nil) != tc.valid {
			t.Fatalf("budget=%s error=%v", tc.budget, err)
		}
	}
}
