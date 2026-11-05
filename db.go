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
	"github.com/go-sql-driver/mysql"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Audit struct {
	err  error
	mu   sync.Mutex
	file *os.File
}

func (a *Audit) Write(kind string, value any) {
	if a == nil || a.file == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		if err := json.NewEncoder(a.file).Encode(map[string]any{"time": time.Now().UTC(), "kind": kind, "value": value}); err != nil {
			a.err = fmt.Errorf("audit log write failed: %w; further SQL submissions stopped", err)
		}
	}
}

func (a *Audit) Err() error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

type Store struct {
	metricTables sync.Map // table ID -> bool; populated by Diagnose before scanning.
	db           *sql.DB
	timeout      time.Duration
	limit        int
	audit        *Audit
}
type Region struct {
	Manifest     sql.NullInt64
	ID           uint64
	Files, Bytes uint64
	Min, Max     sql.NullInt64
}
type Diagnosis struct {
	Metric     bool
	Table      string
	ID         uint64
	Regions    []Region
	Start, End time.Time
	Window     string
}

func (d Diagnosis) Totals() (n, b uint64) {
	for _, r := range d.Regions {
		n += r.Files
		b += r.Bytes
	}
	return
}

type File struct {
	Region   uint64
	ID       string
	Min, Max int64
	Bytes    uint64
}
type Window struct {
	Start, End time.Time
	Files      []File
	Limited    bool
	State      string
	Calls      int
}

func (w Window) Fingerprint() string {
	s := make([]string, 0, len(w.Files))
	for _, f := range w.Files {
		s = append(s, fmt.Sprintf("%d/%s", f.Region, f.ID))
	}
	sort.Strings(s)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(s, "\n"))))
}
func (w Window) Count() string {
	if w.Limited {
		return fmt.Sprintf(">=%d", len(w.Files))
	}
	return strconv.Itoa(len(w.Files))
}
func (w Window) Bytes() uint64 {
	var b uint64
	for _, f := range w.Files {
		b += f.Bytes
	}
	return b
}
func literal(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "''") + "'"
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
func (s *Store) Diagnose(ctx context.Context, table, database string) (Diagnosis, error) {
	d := Diagnosis{Table: table}
	if strings.TrimSpace(table) == "" {
		return d, fmt.Errorf("table is required; enter a table name in database %s", database)
	}
	q := "SELECT table_id, engine, create_options FROM information_schema.tables WHERE table_schema = " + literal(database) + " AND table_name = " + literal(table) + " LIMIT 2"
	var ids []uint64
	var engine, options sql.NullString
	err := s.query(ctx, "Find table", q, func(rows *sql.Rows) error {
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
		return d, err
	}
	if len(ids) != 1 {
		return d, fmt.Errorf("table %s.%s was not found uniquely; check database, table name, and permissions", database, table)
	}
	d.ID = ids[0]
	if physical := physicalTable(options.String); physical != "" {
		return d, &LogicalTableError{Table: table, Physical: physical}
	}

	d.Metric = strings.EqualFold(engine.String, "metric")
	s.metricTables.Store(d.ID, d.Metric)
	d.Regions, err = s.Regions(ctx, d.ID)
	if err != nil {
		return d, err
	}
	if len(d.Regions) == 0 {
		return d, fmt.Errorf("no physical regions for %s; select the physical Mito table, not a logical metric table", table)
	}
	for _, edge := range []struct {
		col, order string
		dest       *time.Time
	}{{"min_ts", "ASC", &d.Start}, {"max_ts", "DESC", &d.End}} {
		q = fmt.Sprintf("SELECT CAST(%s AS BIGINT) AS boundary_ns FROM information_schema.ssts_manifest WHERE %s ORDER BY %s %s LIMIT 1", edge.col, s.manifestScope(d.ID), edge.col, edge.order)
		err = s.query(ctx, "Find SST time boundary", q, func(rows *sql.Rows) error {
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
			return d, err
		}
	}
	d.End = d.End.Add(time.Nanosecond)
	q = fmt.Sprintf("SELECT compaction_time_window FROM information_schema.region_info WHERE region_id = %d LIMIT 1", d.Regions[0].ID)
	err = s.query(ctx, "Read TWCS window", q, func(rows *sql.Rows) error {
		if rows.Next() {
			var value sql.NullString
			if e := rows.Scan(&value); e != nil {
				return e
			}
			d.Window = value.String
		}
		return nil
	})
	if err != nil {
		return d, err
	}
	if d.Window == "" {
		d.Window = "unknown"
	}
	s.audit.Write("diagnosis", d)
	return d, nil
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

func (s *Store) Probe(ctx context.Context, id uint64, start, end time.Time) (Window, error) {
	w := Window{Start: start, End: end, State: "scanned"}
	// Assign each SST by max_ts, matching TWCS's bucket assignment. No aggregate
	// outside a LIMIT: a capped result is a lower bound, never an exact count.
	q := fmt.Sprintf("SELECT region_id, file_id, CAST(min_ts AS BIGINT) AS min_ns, CAST(max_ts AS BIGINT) AS max_ns, file_size FROM information_schema.ssts_manifest WHERE %s AND max_ts >= %s::TIMESTAMP_NS AND max_ts < %s::TIMESTAMP_NS ORDER BY max_ts ASC, min_ts ASC LIMIT %d", s.manifestScope(id), literal(stamp(start)), literal(stamp(end)), s.limit)
	err := s.query(ctx, "Scan manifest window "+stamp(start)+" .. "+stamp(end), q, func(rows *sql.Rows) error {
		for rows.Next() {
			var f File
			if e := rows.Scan(&f.Region, &f.ID, &f.Min, &f.Max, &f.Bytes); e != nil {
				return e
			}
			w.Files = append(w.Files, f)
		}
		return nil
	})
	w.Limited = len(w.Files) >= s.limit
	if w.Limited {
		w.State = "limited (lower bound)"
	}
	if err == nil {
		s.audit.Write("manifest-snapshot", w)
	}
	return w, err
}
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
		var me *mysql.MySQLError
		// RegionBusy maps to MySQL ER_INTERNAL_ERROR (1815), shared by other
		// errors. Require the exact upstream rejection message as well.
		if len(d.Regions) == 1 && errors.As(err, &me) && me.Number == 1815 && manualBusy.MatchString(me.Message) {
			return &AdminError{Kind: "admin_busy", Cause: err}
		}
		return &AdminError{Kind: "admin_unknown", Cause: err}
	}
	s.audit.Write("admin-return", map[string]any{"start": w.Start, "end": w.End})
	return nil
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

type LogicalTableError struct{ Table, Physical string }

func (e *LogicalTableError) Error() string {
	return fmt.Sprintf("%s is a logical metric table. SST and manifest statistics are N/A; storage is shared on physical table %s. No compaction was submitted", e.Table, e.Physical)
}

type TableChoice struct {
	Name, Engine string
	ID           uint64
}

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

type AdminError struct {
	Kind  string
	Cause error
}

func (e *AdminError) Error() string {
	if e.Kind == "admin_busy" {
		return fmt.Sprintf("ADMIN rejected: another manual compaction is running: %v. No automatic retry; inspect progress before resuming", e.Cause)
	}
	return fmt.Sprintf("ADMIN failed or outcome is unknown: %v. No automatic retry; server work may continue", e.Cause)
}
func (e *AdminError) Unwrap() error { return e.Cause }
