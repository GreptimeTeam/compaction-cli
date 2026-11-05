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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuditFailurePreventsSQLSubmission(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "audit"))
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	s := &Store{audit: &Audit{file: f}, timeout: time.Second}
	// A nil DB deliberately proves the failure is returned before any SQL access.
	err = s.Compact(context.Background(), Diagnosis{Table: "events"}, Window{}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "audit log write failed") {
		t.Fatalf("error=%v", err)
	}
	if _, err = s.Regions(context.Background(), 1); err == nil {
		t.Fatal("continued querying after audit failure")
	}
}
