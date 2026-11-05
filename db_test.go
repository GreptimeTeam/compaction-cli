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
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	if err == nil || !errors.Is(err, errAuditFailed) {
		t.Fatalf("error=%v", err)
	}
	if _, err = s.Regions(context.Background(), 1); err == nil {
		t.Fatal("continued querying after audit failure")
	}
}

// failingAudit models a disk-full error at a chosen event, after earlier writes succeed.
type failingAudit struct{ kind string }

func (a *failingAudit) Write(data []byte) (int, error) {
	var event struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return 0, err
	}
	if event.Kind == a.kind {
		return 0, syscall.ENOSPC
	}
	return len(data), nil
}
func (*failingAudit) Close() error { return nil }

// auditConnector returns deterministic SQL results through database/sql itself.
type auditConnector struct {
	queryErr error
	queries  int
}

func (c *auditConnector) Connect(context.Context) (driver.Conn, error) {
	return &auditConn{connector: c}, nil
}
func (*auditConnector) Driver() driver.Driver { return auditDriver{} }

type auditDriver struct{}

func (auditDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }

type auditConn struct{ connector *auditConnector }

func (*auditConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (*auditConn) Close() error                        { return nil }
func (*auditConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected transaction") }
func (*auditConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}
func (c *auditConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	c.connector.queries++
	if c.connector.queryErr != nil {
		return nil, c.connector.queryErr
	}
	var values []driver.Value
	switch {
	case strings.Contains(query, "SELECT table_name"):
		values = []driver.Value{"events", int64(1), "mito"}
	case strings.Contains(query, "SELECT table_id"):
		values = []driver.Value{int64(1), "mito", ""}
	case strings.Contains(query, "region_statistics"):
		values = []driver.Value{int64(1), int64(1), int64(10), int64(10), int64(0), int64(1)}
	case strings.Contains(query, "boundary_ns"):
		values = []driver.Value{int64(1)}
	case strings.Contains(query, "region_info"):
		values = []driver.Value{"6h"}
	default:
		values = []driver.Value{int64(1), "file", int64(10), int64(0), int64(1)}
	}
	return &auditRows{values: values}, nil
}

type auditRows struct {
	values []driver.Value
	read   bool
}

func (r *auditRows) Columns() []string { return make([]string, len(r.values)) }
func (*auditRows) Close() error        { return nil }
func (r *auditRows) Next(dest []driver.Value) error {
	if r.read {
		return io.EOF
	}
	copy(dest, r.values)
	r.read = true
	return nil
}

func TestFinalAuditFailuresPropagate(t *testing.T) {
	for _, kind := range []string{"query-result", "diagnosis", "manifest-snapshot", "admin-return"} {
		t.Run(kind, func(t *testing.T) {
			connector := &auditConnector{}
			db := sql.OpenDB(connector)
			defer func() { _ = db.Close() }()
			store := &Store{db: db, timeout: time.Second, limit: 200, audit: &Audit{file: &failingAudit{kind: kind}}}
			var err error
			switch kind {
			case "query-result":
				_, err = store.Tables(context.Background(), "public")
			case "diagnosis":
				_, err = store.Diagnose(context.Background(), "events", "public")
			case "manifest-snapshot":
				_, err = store.Probe(context.Background(), 1, time.Unix(0, 0), time.Unix(1, 0))
			case "admin-return":
				err = store.Compact(context.Background(), Diagnosis{Table: "events"}, Window{}, time.Second)
			}
			if !errors.Is(err, errAuditFailed) || !errors.Is(err, syscall.ENOSPC) || problem(err, "").Exit != 7 {
				t.Fatalf("error=%v", err)
			}
			before := connector.queries
			if _, err = store.Tables(context.Background(), "public"); !errors.Is(err, errAuditFailed) || connector.queries != before {
				t.Fatalf("SQL continued after audit failure: %v", err)
			}
		})
	}
}

func TestQueryAndAuditErrorsAreBothPreserved(t *testing.T) {
	cause := errors.New("SQL failed")
	db := sql.OpenDB(&auditConnector{queryErr: cause})
	defer func() { _ = db.Close() }()
	store := &Store{db: db, timeout: time.Second, audit: &Audit{file: &failingAudit{kind: "query-result"}}}
	_, err := store.Tables(context.Background(), "public")
	if !errors.Is(err, cause) || !errors.Is(err, errAuditFailed) {
		t.Fatalf("error=%v", err)
	}
}
