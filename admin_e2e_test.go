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
