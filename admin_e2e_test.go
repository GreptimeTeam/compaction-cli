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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBusyRejectionE2E(t *testing.T) {
	port, err := strconv.Atoi(os.Getenv("COMPACTCTL_E2E_PORT"))
	if err != nil {
		t.Skip("requires dedicated standalone")
	}
	o, _ := parseOptions([]string{"--port", strconv.Itoa(port)})
	const clients = 16
	stores := make([]*Store, clients)
	for i := range stores {
		db, e := connect(o, "")
		if e != nil {
			t.Fatal(e)
		}
		defer func() { _ = db.Close() }()
		stores[i] = &Store{db: db, timeout: 10 * time.Second, limit: 200}
	}
	d, err := stores[0].Diagnose(context.Background(), "busy_bench", "public")
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]string, clients)
	busy := make([]bool, clients)
	for i := range stores {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			e := stores[i].Compact(context.Background(), d, Window{Start: d.Start, End: d.End}, time.Minute)
			if e == nil {
				results[i] = "accepted"
				return
			}
			results[i] = e.Error()
			var ae *AdminError
			busy[i] = errors.As(e, &ae) && ae.Kind == "admin_busy"
		}(i)
	}
	close(start)
	wg.Wait()
	var busyCount, accepted int
	for i := range results {
		if busy[i] {
			busyCount++
		}
		if results[i] == "accepted" {
			accepted++
		}
	}
	if path := os.Getenv("COMPACTCTL_BUSY_RESULTS"); path != "" {
		b, _ := json.MarshalIndent(results, "", "  ")
		if e := os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if busyCount == 0 || accepted == 0 {
		t.Fatalf("accepted=%d busy=%d responses=%s", accepted, busyCount, strings.Join(results, "\n"))
	}
	fmt.Printf("accepted=%d classified_busy=%d total=%d\n", accepted, busyCount, clients)
}

func TestManifestTieOrderingE2E(t *testing.T) {
	port := os.Getenv("COMPACTCTL_E2E_PORT")
	if port == "" {
		t.Skip("requires dedicated standalone with compaction disabled")
	}
	options, err := parseOptions([]string{"--port", port})
	if err != nil {
		t.Fatal(err)
	}
	db, err := connect(options, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, timeout: 10 * time.Second, limit: 100}
	ctx := context.Background()
	for _, value := range []string{"plain", "quote'", `back\slash`, `both\'`} {
		var got string
		if err := db.QueryRowContext(ctx, "SELECT "+literal(value)).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != value {
			t.Fatalf("literal round trip: got %q want %q", got, value)
		}
	}
	diagnosis, err := store.Diagnose(ctx, "tied_bench", "public")
	if err != nil {
		t.Fatal(err)
	}
	all, err := store.Probe(ctx, diagnosis.ID, diagnosis.Start, diagnosis.End)
	if err != nil || all.Limited || len(all.Files) != 6 {
		t.Fatalf("sample=%+v err=%v", all, err)
	}
	for _, file := range all.Files {
		if file.Min != 0 || file.Max != 0 {
			t.Fatal("ordinary probe fetched unused bounds")
		}
	}
	auditFile, err := os.Create(filepath.Join(t.TempDir(), "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = auditFile.Close() }()
	store.audit = &Audit{file: auditFile}
	audited, err := store.Probe(ctx, diagnosis.ID, diagnosis.Start, diagnosis.End)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range audited.Files {
		if file.Min != diagnosis.Start.UnixNano() || file.Max != file.Min {
			t.Fatalf("missing audited bounds: %+v", file)
		}
	}
	store.audit = nil
	sort.Slice(all.Files, func(i, j int) bool { return all.Files[i].ID < all.Files[j].ID })
	store.limit = 2
	for range 3 {
		sample, err := store.Probe(ctx, diagnosis.ID, diagnosis.Start, diagnosis.End)
		if err != nil || !sample.Limited || len(sample.Files) != 2 {
			t.Fatalf("sample=%+v err=%v", sample, err)
		}
		for i, file := range sample.Files {
			if file.ID != all.Files[i].ID {
				t.Fatalf("unstable tie order: got %s want %s", file.ID, all.Files[i].ID)
			}
		}
	}
}

// TestExampleConfig validates the documented entry point without a server.
func TestExampleConfig(t *testing.T) {
	options, err := parseOptions([]string{"--config", "config.example.toml"})
	if err != nil {
		t.Fatal(err)
	}
	if err := options.Validate(); err != nil {
		t.Fatal(err)
	}
}
