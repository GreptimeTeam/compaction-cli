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
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Audit records SQL activity and retains write errors to stop further submissions.
type Audit struct {
	err  error
	mu   sync.Mutex
	file *os.File
}

// Write appends an audit event unless logging is disabled or has failed.
func (a *Audit) Write(kind string, value any) {
	if a == nil || a.file == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		if err := json.NewEncoder(a.file).Encode(map[string]any{"time": time.Now().UTC(), "kind": kind, "value": value}); err != nil {
			a.err = fmt.Errorf("%w: %w; further SQL submissions stopped", errAuditFailed, err)
		}
	}
}

// Err returns the first audit write failure, if any.
func (a *Audit) Err() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Store performs bounded MySQL reads and serial compaction requests.
type Store struct {
	metricTables sync.Map // table ID -> bool; populated by Diagnose before scanning.
	db           *sql.DB
	timeout      time.Duration
	limit        int
	audit        *Audit
}

// Region contains one region heartbeat's SST and manifest statistics.
type Region struct {
	Manifest     sql.NullInt64
	ID           uint64
	Files, Bytes uint64
	Min, Max     sql.NullInt64 // Included in diagnosis JSON and saved schema-v1 plans.
}

// Diagnosis identifies physical table storage and its observed time bounds.
type Diagnosis struct {
	Metric     bool
	Table      string
	ID         uint64
	Regions    []Region
	Start, End time.Time
	Window     string
}

// Totals sums file counts and bytes across the reported regions.
func (d Diagnosis) Totals() (n, b uint64) {
	for _, r := range d.Regions {
		n += r.Files
		b += r.Bytes
	}
	return
}

// File identifies a sampled SST. Nanosecond bounds are populated for audit output.
type File struct {
	Region   uint64
	ID       string
	Min, Max int64
	Bytes    uint64
}

// Window holds a bounded manifest sample for a half-open max_ts range.
type Window struct {
	Start, End time.Time
	Files      []File
	Limited    bool
	State      string
	Calls      int // Reserved zero-valued field in manifest audit snapshots.
}

// Fingerprint hashes sorted region/file identities to detect sample changes.
func (w Window) Fingerprint() string {
	s := make([]string, 0, len(w.Files))
	for _, f := range w.Files {
		s = append(s, fmt.Sprintf("%d/%s", f.Region, f.ID))
	}
	sort.Strings(s)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(s, "\n"))))
}

// Count formats the observed count, marking capped samples as lower bounds.
func (w Window) Count() string {
	if w.Limited {
		return fmt.Sprintf(">=%d", len(w.Files))
	}
	return strconv.Itoa(len(w.Files))
}

// Bytes sums sizes of returned files, excluding any unsampled entries.
func (w Window) Bytes() uint64 {
	var b uint64
	for _, f := range w.Files {
		b += f.Bytes
	}
	return b
}
func literal(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }
func (s *Store) query(ctx context.Context, operation, q string, read func(*sql.Rows) error) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	begin := time.Now()
	s.audit.Write("query", map[string]any{"operation": operation, "sql": q})
	if err := s.audit.Err(); err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, q)
	if err == nil {
		defer func() { _ = rows.Close() }()
		err = read(rows)
		if err == nil {
			err = rows.Err()
		}
	}
	if err != nil {
		err = fmt.Errorf("%s: %w", operation, err)
	}
	s.audit.Write("query-result", map[string]any{"operation": operation, "elapsed": time.Since(begin).String(), "ok": err == nil})
	return err
}

// Regions reads up to 257 rows and rejects missing, duplicate or excess regions.
func (s *Store) Regions(ctx context.Context, id uint64) ([]Region, error) {
	var result []Region
	q := fmt.Sprintf("SELECT region_id, sst_num, sst_size, manifest_size, CAST(min_timestamp AS BIGINT), CAST(max_timestamp AS BIGINT) FROM information_schema.region_statistics WHERE table_id = %d LIMIT 257", id)
	err := s.query(ctx, "Read region statistics", q, func(rows *sql.Rows) error {
		for rows.Next() {
			var r Region
			var n, b sql.NullInt64
			if err := rows.Scan(&r.ID, &n, &b, &r.Manifest, &r.Min, &r.Max); err != nil {
				return err
			}
			if !n.Valid || !b.Valid || n.Int64 < 0 || b.Int64 < 0 {
				return fmt.Errorf("region %d has unavailable SST statistics; retry after heartbeat refresh", r.ID)
			}
			r.Files = uint64(n.Int64)
			r.Bytes = uint64(b.Int64)
			result = append(result, r)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(result) >= 257 {
		return nil, fmt.Errorf("table exceeds the 256-region diagnostic budget; select a smaller table")
	}
	seen := map[uint64]bool{}
	for _, r := range result {
		if seen[r.ID] {
			return nil, fmt.Errorf("duplicate statistics for region %d; cannot safely report exact table totals", r.ID)
		}
		seen[r.ID] = true
	}
	return result, nil
}

// Diagnose resolves physical storage and reads bounded statistics and time limits.
func (s *Store) Diagnose(ctx context.Context, table, database string) (Diagnosis, error) {
	diagnosis := Diagnosis{Table: table}
	if strings.TrimSpace(table) == "" {
		return diagnosis, fmt.Errorf("table is required; enter a table name in database %s", database)
	}
	query := "SELECT table_id, engine, create_options FROM information_schema.tables WHERE table_schema = " + literal(database) + " AND table_name = " + literal(table) + " LIMIT 2"
	var ids []uint64
	var engine, options sql.NullString
	err := s.query(ctx, "Find table", query, func(rows *sql.Rows) error {
		for rows.Next() {
			var id uint64
			if e := rows.Scan(&id, &engine, &options); e != nil {
				return e
			}
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return diagnosis, err
	}
	if len(ids) != 1 {
		return diagnosis, fmt.Errorf("table %s.%s was not found uniquely; check database, table name, and permissions", database, table)
	}
	diagnosis.ID = ids[0]
	if physical := physicalTable(options.String); physical != "" {
		return diagnosis, &LogicalTableError{Table: table, Physical: physical}
	}

	diagnosis.Metric = strings.EqualFold(engine.String, "metric")
	s.metricTables.Store(diagnosis.ID, diagnosis.Metric)
	diagnosis.Regions, err = s.Regions(ctx, diagnosis.ID)
	if err != nil {
		return diagnosis, err
	}
	if len(diagnosis.Regions) == 0 {
		return diagnosis, fmt.Errorf("no physical regions for %s; select the physical Mito table, not a logical metric table", table)
	}
	for _, edge := range []struct {
		col, order string
		dest       *time.Time
	}{{"min_ts", "ASC", &diagnosis.Start}, {"max_ts", "DESC", &diagnosis.End}} {
		query = fmt.Sprintf("SELECT CAST(%s AS BIGINT) AS boundary_ns FROM information_schema.ssts_manifest WHERE %s ORDER BY %s %s LIMIT 1", edge.col, s.manifestScope(diagnosis.ID), edge.col, edge.order)
		err = s.query(ctx, "Find SST time boundary", query, func(rows *sql.Rows) error {
			if !rows.Next() {
				return fmt.Errorf("table has no SST files; flush existing data or select another table")
			}
			var n sql.NullInt64
			if e := rows.Scan(&n); e != nil {
				return e
			}
			if !n.Valid {
				return fmt.Errorf("SST timestamp boundary is NULL")
			}
			*edge.dest = time.Unix(0, n.Int64).UTC()
			return nil
		})
		if err != nil {
			return diagnosis, err
		}
	}
	diagnosis.End = diagnosis.End.Add(time.Nanosecond)
	query = fmt.Sprintf("SELECT compaction_time_window FROM information_schema.region_info WHERE region_id = %d LIMIT 1", diagnosis.Regions[0].ID)
	err = s.query(ctx, "Read TWCS window", query, func(rows *sql.Rows) error {
		if rows.Next() {
			var value sql.NullString
			if e := rows.Scan(&value); e != nil {
				return e
			}
			diagnosis.Window = value.String
		}
		return nil
	})
	if err != nil {
		return diagnosis, err
	}
	if diagnosis.Window == "" {
		diagnosis.Window = "unknown"
	}
	s.audit.Write("diagnosis", diagnosis)
	return diagnosis, nil
}

// Metric metadata SSTs have timestamp zero and are not time-series data.
// region_group is exposed by ssts_manifest; filtering it retains all data
// regions, including regions added since the initial diagnostic.
func (s *Store) manifestScope(id uint64) string {
	q := fmt.Sprintf("table_id = %d", id)
	if metric, ok := s.metricTables.Load(id); ok && metric.(bool) {
		q += " AND region_group = 0"
	}
	return q
}

// Probe samples files by max_ts in [start, end), subject to the configured LIMIT.
func (s *Store) Probe(ctx context.Context, id uint64, start, end time.Time) (Window, error) {
	window := Window{Start: start, End: end, State: "scanned"}
	// Assign each SST by max_ts, matching TWCS's bucket assignment. No aggregate
	// outside a LIMIT: a capped result is a lower bound, never an exact count.
	// File bounds are audit detail; ordinary probes need only identity and size.
	includeBounds := s.audit != nil && s.audit.file != nil
	columns := "region_id, file_id, file_size"
	if includeBounds {
		columns += ", CAST(min_ts AS BIGINT) AS min_ns, CAST(max_ts AS BIGINT) AS max_ns"
	}
	q := fmt.Sprintf("SELECT %s FROM information_schema.ssts_manifest WHERE %s AND max_ts >= %s::TIMESTAMP_NS AND max_ts < %s::TIMESTAMP_NS ORDER BY max_ts ASC, min_ts ASC, region_id ASC, file_id ASC LIMIT %d", columns, s.manifestScope(id), literal(stamp(start)), literal(stamp(end)), s.limit)
	err := s.query(ctx, "Scan manifest window "+stamp(start)+" .. "+stamp(end), q, func(rows *sql.Rows) error {
		for rows.Next() {
			var f File
			destinations := []any{&f.Region, &f.ID, &f.Bytes}
			if includeBounds {
				destinations = append(destinations, &f.Min, &f.Max)
			}
			if e := rows.Scan(destinations...); e != nil {
				return e
			}
			window.Files = append(window.Files, f)
		}
		return nil
	})
	window.Limited = len(window.Files) >= s.limit
	if window.Limited {
		window.State = "limited (lower bound)"
	}
	if err == nil {
		s.audit.Write("manifest-snapshot", window)
	}
	return window, err
}

// Compact submits one ADMIN request and distinguishes known rejection from uncertainty.
func (s *Store) Compact(ctx context.Context, d Diagnosis, w Window, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	options := fmt.Sprintf("parallelism=1,start_time=%s,end_time=%s", stamp(w.Start), stamp(w.End))
	q := fmt.Sprintf("ADMIN COMPACT_TABLE(%s, 'twcs', %s)", literal(d.Table), literal(options))
	s.audit.Write("admin-submit", map[string]any{"sql": q})
	if err := s.audit.Err(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, q)
	if err != nil {
		return classifyAdminError(err, len(d.Regions))
	}
	s.audit.Write("admin-return", map[string]any{"start": w.Start, "end": w.End})
	return nil
}

// A multi-region request may partially succeed even if one region reports busy.
// Only the known single-region rejection establishes that no work was accepted.
func classifyAdminError(err error, regionCount int) error {
	if err == nil {
		return nil
	}
	var mysqlErr *mysql.MySQLError
	if regionCount == 1 && errors.As(err, &mysqlErr) && mysqlErr.Number == 1815 && manualBusy.MatchString(mysqlErr.Message) {
		return &AdminError{Kind: "admin_busy", Cause: err}
	}
	return &AdminError{Kind: "admin_unknown", Cause: err}
}

// Table options are rendered as space-separated key=value pairs by GreptimeDB.
// Only offer a switch when a simple physical-table name can be read explicitly.
func physicalTable(options string) string {
	for _, part := range strings.Fields(options) {
		if strings.HasPrefix(part, "on_physical_table=") {
			return strings.TrimPrefix(part, "on_physical_table=")
		}
	}
	return ""
}

// LogicalTableError identifies a logical table whose physical scope must be selected.
type LogicalTableError struct{ Table, Physical string }

// Error explains the required physical table selection.
func (e *LogicalTableError) Error() string {
	return fmt.Sprintf("%s is a logical metric table. SST and manifest statistics are N/A; storage is shared on physical table %s. No compaction was submitted", e.Table, e.Physical)
}

// TableChoice identifies a table returned by bounded discovery.
type TableChoice struct {
	Name, Engine string
	ID           uint64
}

// Tables lists at most 200 tables in the selected database.
func (s *Store) Tables(ctx context.Context, database string) ([]TableChoice, error) {
	var result []TableChoice
	q := "SELECT table_name, table_id, engine FROM information_schema.tables WHERE table_schema = " + literal(database) + " ORDER BY table_name LIMIT 200"
	err := s.query(ctx, "List tables (first 200)", q, func(rows *sql.Rows) error {
		for rows.Next() {
			var c TableChoice
			var id sql.NullInt64
			var engine sql.NullString
			if err := rows.Scan(&c.Name, &id, &engine); err != nil {
				return err
			}
			if !id.Valid {
				continue
			}
			c.ID = uint64(id.Int64)
			c.Engine = engine.String
			result = append(result, c)
		}
		return nil
	})
	return result, err
}

var manualBusy = regexp.MustCompile(`Manual compaction is already running for region [0-9]+(?:\([0-9]+, [0-9]+\))?\.`)

// AdminError preserves the SQL cause and the known or uncertain request outcome.
type AdminError struct {
	Kind  string
	Cause error
}

// Error describes the ADMIN outcome and its underlying SQL error.
func (e *AdminError) Error() string {
	if e.Kind == "admin_busy" {
		return fmt.Sprintf("ADMIN rejected: another manual compaction is running: %v. No automatic retry; inspect progress before resuming", e.Cause)
	}
	return fmt.Sprintf("ADMIN failed or outcome is unknown: %v. No automatic retry; server work may continue", e.Cause)
}

// Unwrap exposes the underlying SQL error.
func (e *AdminError) Unwrap() error { return e.Cause }
