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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/sys/unix"
)

const schemaVersion = 1

var version = "dev"
var commit = "unknown"

// PlannedWindow is a persisted range summary; capped counts are lower bounds.
type PlannedWindow struct {
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	Count   int       `json:"sst_count"`
	Limited bool      `json:"limited"`
	Bytes   uint64    `json:"observed_bytes"`
}

// Plan records reviewed scope and settings for later execution.
type Plan struct {
	Version   int             `json:"version"`
	Created   time.Time       `json:"created"`
	Options   Options         `json:"options"`
	Diagnosis Diagnosis       `json:"diagnosis"`
	Windows   []PlannedWindow `json:"windows"`
}

// State is the durable checkpoint used to resume an execution.
type State struct {
	Version int           `json:"version"`
	Updated time.Time     `json:"updated"`
	Status  string        `json:"status"`
	Plan    Plan          `json:"plan"`
	Index   int           `json:"next_window"`
	Calls   int           `json:"admin_attempts"`
	Error   *CommandError `json:"error,omitempty"`
}

// CommandError describes a CLI failure and its recovery requirements.
type CommandError struct {
	Code           string `json:"code"`
	Message        string `json:"message"`
	Retryable      bool   `json:"retryable"`
	OutcomeUnknown bool   `json:"outcome_unknown"`
	PhysicalTable  string `json:"physical_table,omitempty"`
	Exit           int    `json:"exit_code"`
}

// Error returns the user-facing failure message.
func (p *CommandError) Error() string { return p.Message }
func problem(err error, password string) *CommandError {
	var failure *CommandError
	if errors.As(err, &failure) {
		return failure
	}
	failure = &CommandError{Code: "query_failed", Message: friendly(err, password), Exit: 3}
	var logical *LogicalTableError
	var admin *AdminError
	switch {
	case errors.As(err, &logical):
		failure.Code = "logical_table"
		failure.PhysicalTable = logical.Physical
	case errors.As(err, &admin):
		failure.Code = admin.Kind
		failure.Retryable = admin.Kind == "admin_busy"
		failure.OutcomeUnknown = !failure.Retryable
		failure.Exit = 4
		if failure.OutcomeUnknown {
			failure.Exit = 5
		}
	case errors.Is(err, context.Canceled):
		failure.Code = "interrupted"
		failure.Exit = 130
	case strings.Contains(err.Error(), "audit log write failed"):
		failure.Code = "audit_failed"
		failure.Exit = 7
	case strings.Contains(err.Error(), "observation timed out"):
		failure.Code = "observation_timeout"
		failure.Exit = 6
	case strings.Contains(err.Error(), "no progress observed"):
		failure.Code = "no_progress"
		failure.Exit = 6
	case strings.Contains(err.Error(), "ADMIN budget reached"):
		failure.Code = "budget_exhausted"
		failure.Exit = 6
	}
	return failure
}
func invalid(s string) error { return &CommandError{Code: "invalid_arguments", Message: s, Exit: 2} }
func stateError(err error) error {
	return &CommandError{Code: "state_io", Message: err.Error(), Exit: 7}
}

// Printer writes versioned events and retains the first output error.
type Printer struct {
	Format string
	Out    io.Writer
	Err    error
	header bool
}

// Emit writes an event; JSON output includes only final events.
func (p *Printer) Emit(kind string, data any, final bool) {
	if p.Err != nil {
		return
	}
	envelope := map[string]any{"version": schemaVersion, "type": kind, "time": time.Now().UTC(), "data": data}
	if v, ok := data.(Progress); ok {
		var total *uint64
		if v.Regions != nil {
			n, _ := (Diagnosis{Regions: v.Regions}).Totals()
			total = &n
		}
		envelope["data"] = map[string]any{"window_index": v.Index, "admin_attempts": v.Calls, "start": v.Window.Start, "end": v.Window.End, "sst_count": len(v.Window.Files), "limited": v.Window.Limited, "observed_bytes": v.Window.Bytes(), "table_sst_count": total, "state": v.Window.State, "message": v.Message}
	}

	switch p.Format {
	case "json":
		if final {
			p.Err = json.NewEncoder(p.Out).Encode(envelope)
		}
	case "jsonl":
		p.Err = json.NewEncoder(p.Out).Encode(envelope)
	case "markdown":
		p.Err = p.markdown(kind, data, final)
	}
}
func md(s string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ", "\r", " ", "`", "'").Replace(s)
}
func (p *Printer) markdown(kind string, data any, final bool) error {
	var output strings.Builder
	switch value := data.(type) {
	case []TableChoice:
		output.WriteString("| Table | ID | Engine |\n|---|---:|---|\n")
		for _, t := range value {
			fmt.Fprintf(&output, "| %s | %d | %s |\n", md(t.Name), t.ID, md(t.Engine))
		}
	case Diagnosis:
		writeDiagnosisMarkdown(&output, value)
	case Plan:
		fmt.Fprintf(&output, "Plan: **%s**, %d windows; ADMIN target ≤ %d SSTs/window. No ADMIN submitted.\n\n| Start | End (exclusive) | SSTs | Observed MiB | Observed avg KiB |\n|---|---|---:|---:|---:|\n", md(value.Diagnosis.Table), len(value.Windows), value.Options.Target)
		for _, w := range value.Windows {
			prefix := ""
			if w.Limited {
				prefix = ">="
			}
			avg := 0.0
			if w.Count > 0 {
				avg = float64(w.Bytes) / float64(w.Count) / 1024
			}
			fmt.Fprintf(&output, "| %s | %s | %s%d | %.3f | %.2f |\n", stamp(w.Start), stamp(w.End), prefix, w.Count, float64(w.Bytes)/(1<<20), avg)
		}
	case Progress:
		if !p.header {
			output.WriteString("| Window | ADMIN attempts | Observed SSTs | Whole-table SSTs | State |\n|---:|---:|---:|---:|---|\n")
			p.header = true
		}
		total := "N/A"
		if value.Regions != nil {
			var n uint64
			for _, r := range value.Regions {
				n += r.Files
			}
			total = fmt.Sprint(n)
		}
		fmt.Fprintf(&output, "| %d | %d | %s | %s | %s |\n", value.Index+1, value.Calls, value.Window.Count(), total, md(value.Window.State))
	case State:
		fmt.Fprintf(&output, "\n**%s** — %d/%d windows complete; %d ADMIN attempts.\n", md(value.Status), value.Index, len(value.Plan.Windows), value.Calls)
		if value.Error != nil {
			fmt.Fprintf(&output, "\nError `%s`: %s\n", value.Error.Code, md(value.Error.Message))
		}
	case *CommandError:
		fmt.Fprintf(&output, "Error **%s**: %s\n", value.Code, md(value.Message))
		if value.PhysicalTable != "" {
			fmt.Fprintf(&output, "\nPhysical table: `%s` (shared storage; select explicitly).\n", md(value.PhysicalTable))
		}
	default:
		if final {
			raw, _ := json.MarshalIndent(data, "", "  ")
			fmt.Fprintf(&output, "```json\n%s\n```\n", raw)
		}
	}
	_, err := io.WriteString(p.Out, output.String())
	return err
}

func writeDiagnosisMarkdown(b *strings.Builder, v Diagnosis) {
	n, size := v.Totals()
	var manifests int64
	known := true
	for _, r := range v.Regions {
		known = known && r.Manifest.Valid
		manifests += r.Manifest.Int64
	}
	m := "N/A"
	if known {
		m = fmt.Sprintf("%.2f MiB", float64(manifests)/(1<<20))
	}
	avg := float64(0)
	if n > 0 {
		avg = float64(size) / float64(n) / 1024
	}
	fmt.Fprintf(b, "| Table | SSTs | SST MiB | Avg KiB | Manifest | TWCS |\n|---|---:|---:|---:|---:|---|\n| %s | %d | %.2f | %.2f | %s | %s |\n\nRange: `%s` → `%s` (exclusive).\n", md(v.Table), n, float64(size)/(1<<20), avg, m, md(v.Window), stamp(v.Start), stamp(v.End))
	if v.Metric {
		b.WriteString("\nTotals include metadata SSTs; range scans cover data SSTs only.\n")
	}
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(io.LimitReader(f, (16<<20)+1))
	if err != nil {
		return err
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("JSON file exceeds 16 MiB")
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err = dec.Decode(v); err != nil {
		return err
	}
	var extra any
	if err = dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}
func saveJSON(path string, v any, exclusive bool) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if len(data) > 16<<20 {
		return fmt.Errorf("state exceeds 16 MiB")
	}
	if exclusive {
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = f.Write(data)
		if e == nil {
			e = f.Sync()
		}
		ce := f.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
		return syncParent(path)
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".greptime-compaction-cli-state-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer func() { _ = os.Remove(name) }()
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return err
	}
	if ce != nil {
		return ce
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	return syncParent(path)
}

// syncParent persists the directory entry before an ADMIN may rely on its state.
func syncParent(path string) error {
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
}

func validatePlan(p Plan) error {
	if p.Version != schemaVersion || p.Diagnosis.ID == 0 || p.Options.Table != p.Diagnosis.Table || len(p.Windows) == 0 || len(p.Windows) > 4096 {
		return invalid("invalid plan version, identity, or window count")
	}
	if err := p.Options.Validate(); err != nil {
		return invalid(err.Error())
	}
	for i, w := range p.Windows {
		if !w.End.After(w.Start) || (i > 0 && !w.Start.Equal(p.Windows[i-1].End)) {
			return invalid("plan windows must be positive, contiguous, and ordered")
		}
	}
	return nil
}
func lockState(path string) (*os.File, error) {
	f, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another process holds state lock: %w", err)
	}
	return f, nil
}
func usage() {
	fmt.Fprintln(os.Stderr, `greptime-compaction-cli <tables|diagnose|plan|run|resume|status> [options]

Read: tables / diagnose --table NAME
Plan: plan --table NAME --start RFC3339 --end RFC3339 --save plan.json
Run: run --plan plan.json --state run.json --execute
Resume: resume --state run.json --execute [--ack-unknown]
Status: status --state run.json

-o / --output: jsonl (default), json, markdown
Connection and tuning: --config settings.toml, or named flags. Use <command> --help.
Run/resume use connection and tuning stored in the reviewed plan; credentials are never saved.
A busy or uncertain ADMIN is never automatically retried.`)
}

// commandSession owns one invocation's output, state and cancellation flag.
// Saved plan settings remain separate from invocation flags and credentials.
type commandSession struct {
	options  Options
	state    State
	printer  *Printer
	password string
	store    *Store
	stopped  atomic.Bool
}

func (session *commandSession) fail(err error, password string) int {
	failure := problem(err, password)
	session.printer.Emit("error", failure, true)
	if session.printer.Err != nil {
		fmt.Fprintln(os.Stderr, "output failed:", session.printer.Err)
		return 7
	}
	return failure.Exit
}

func readState(path string) (State, error) {
	var state State
	if path == "" {
		return state, invalid("--state is required")
	}
	if err := readJSON(path, &state); err != nil {
		return state, stateError(err)
	}
	if err := validatePlan(state.Plan); err != nil {
		return state, err
	}
	if state.Version != schemaVersion || state.Index < 0 || state.Index > len(state.Plan.Windows) || state.Calls < 0 {
		return state, invalid("invalid execution state")
	}
	return state, nil
}

func validateExecutionFlags(options Options) error {
	if !options.Execute || options.StatePath == "" {
		return invalid("--execute and --state are required")
	}
	for key := range options.Provided {
		switch key {
		case "state", "plan", "execute", "ack-unknown", "o", "output", "p", "password", "password-stdin", "log":
		default:
			return invalid("run/resume use settings saved in the plan; create a new plan to change " + key)
		}
	}
	return nil
}

// prepareExecution is called with the state lock held and re-reads resume state.
func (session *commandSession) prepareExecution(cmd string) error {
	options := session.options
	if cmd == "resume" {
		state, err := readState(options.StatePath)
		if err != nil {
			return err
		}
		if (state.Status == "admin_in_flight" || (state.Error != nil && state.Error.OutcomeUnknown)) && !options.AckUnknown {
			return &CommandError{Code: "acknowledgement_required", Message: "Previous ADMIN outcome is unknown; inspect server progress, then use --ack-unknown to authorize a fresh recheck and possible submission", OutcomeUnknown: true, Exit: 5}
		}
		if state.Error != nil && state.Error.Code == "admin_busy" && time.Since(state.Updated) < state.Plan.Options.Poll {
			return &CommandError{Code: "admin_busy", Message: "Busy cooldown: wait at least one configured poll interval before resume", Retryable: true, Exit: 4}
		}
		session.state = state
		return nil
	}
	if options.PlanPath == "" {
		return invalid("--plan is required")
	}
	if err := readJSON(options.PlanPath, &session.state.Plan); err != nil {
		return stateError(err)
	}
	if err := validatePlan(session.state.Plan); err != nil {
		return err
	}
	session.state.Version = schemaVersion
	session.state.Status = "ready"
	session.state.Updated = time.Now().UTC()
	if err := saveJSON(options.StatePath, session.state, true); err != nil {
		return stateError(err)
	}
	return nil
}

func command(args []string, out io.Writer) int {
	if len(args) == 1 && args[0] == "skill" {
		if _, err := io.WriteString(out, agentSkill); err != nil {
			return 7
		}
		return 0
	}
	if len(args) > 0 && (args[0] == "version" || args[0] == "--version") {
		if _, err := fmt.Fprintf(out, "greptime-compaction-cli %s (%s)\n", version, commit); err != nil {
			return 7
		}
		return 0
	}
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		usage()
		return 0
	}
	options, err := parseOptions(args[1:])
	printer := &Printer{Format: options.Output, Out: out}
	if printer.Format != "json" && printer.Format != "markdown" {
		printer.Format = "jsonl"
	}
	session := &commandSession{options: options, printer: printer}
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		return session.fail(invalid(err.Error()), "")
	}
	return session.dispatch(args[0])
}

func (session *commandSession) dispatch(cmd string) int {
	switch cmd {
	case "tables", "diagnose", "plan":
		return session.runConnected(cmd)
	case "status":
		state, err := readState(session.options.StatePath)
		if err != nil {
			return session.fail(err, "")
		}
		session.printer.Emit("status", state, true)
		if session.printer.Err != nil {
			return 7
		}
		return 0
	case "run", "resume":
		if err := validateExecutionFlags(session.options); err != nil {
			return session.fail(err, "")
		}
		lock, err := lockState(session.options.StatePath)
		if err != nil {
			return session.fail(stateError(err), "")
		}
		defer func() { _ = lock.Close() }()
		if err := session.prepareExecution(cmd); err != nil {
			return session.fail(err, "")
		}
		if cmd == "resume" && session.state.Status == "complete" {
			session.printer.Emit("result", session.state, true)
			if session.printer.Err != nil {
				return 7
			}
			return 0
		}
		return session.runConnected(cmd)
	default:
		return session.fail(invalid("unknown command: "+cmd), "")
	}
}

func connectCommand(o, conn Options) (*sql.DB, string, error) {
	var err error
	password := ""
	if o.PasswordStdin {
		b, e := io.ReadAll(io.LimitReader(os.Stdin, 65537))
		if e != nil || len(b) > 65536 {
			return nil, "", invalid("cannot read password stdin (maximum 64 KiB)")
		}
		password = strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	} else if o.Password {
		password, err = readPassword()
		if err != nil {
			return nil, "", err
		}
	}
	db, err := connect(conn, password)
	if err != nil && !o.Password && !o.PasswordStdin {
		var me *mysql.MySQLError
		if errors.As(err, &me) && (me.Number == 1045 || me.Number == 1698) {
			password, err = readPassword()
			if err == nil {
				db, err = connect(conn, password)
			}
		}
	}
	if err != nil {
		return nil, password, &CommandError{Code: "connection_failed", Message: friendly(err, password), Exit: 3}
	}
	return db, password, nil
}

func (session *commandSession) runConnected(cmd string) int {
	options, state, printer, fail := session.options, &session.state, session.printer, session.fail
	conn := options
	if cmd == "run" || cmd == "resume" {
		conn = state.Plan.Options
	}
	db, password, err := connectCommand(options, conn)
	if err != nil {
		return fail(err, password)
	}
	session.password = password
	defer func() { _ = db.Close() }()
	audit := &Audit{}
	logPath := options.Log
	if logPath != "" {
		audit.file, err = os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return fail(stateError(err), password)
		}
		defer func() { _ = audit.file.Close() }()
	}
	store := &Store{db: db, timeout: conn.Timeout, limit: conn.Limit, audit: audit}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)
	stopped := &session.stopped
	go func() {
		select {
		case <-signals:
			stopped.Store(true)
			if cmd != "run" && cmd != "resume" {
				cancel()
			}
		case <-ctx.Done():
		}
	}()
	if cmd == "tables" {
		tables, e := store.Tables(ctx, options.Database)
		if e != nil {
			return fail(e, password)
		}
		printer.Emit("tables", tables, true)
		if printer.Err != nil {
			return 7
		}
		return 0
	}
	d, err := store.Diagnose(ctx, conn.Table, conn.Database)
	if err != nil {
		return fail(err, password)
	}
	if cmd == "diagnose" {
		printer.Emit("diagnosis", d, true)
		if printer.Err != nil {
			return 7
		}
		return 0
	}
	session.store = store
	if cmd == "plan" {
		return session.plan(ctx, cancel, d)
	}
	return session.execute(ctx, d)
}

func (session *commandSession) plan(ctx context.Context, cancel context.CancelFunc, diagnosis Diagnosis) int {
	options, store, printer, fail, password := session.options, session.store, session.printer, session.fail, session.password
	if options.Start == "" || options.End == "" {
		return fail(invalid("plan requires explicit --start and --end; use diagnose to discover bounds"), password)
	}
	start, err := time.Parse(time.RFC3339Nano, options.Start)
	if err != nil {
		return fail(invalid("invalid --start RFC3339"), password)
	}
	end, err := time.Parse(time.RFC3339Nano, options.End)
	if err != nil {
		return fail(invalid("invalid --end RFC3339"), password)
	}
	windows, err := Scan(ctx, store, diagnosis.ID, start, end, options.RequestWindow, options, func(w Window) {
		printer.Emit("scan", PlannedWindow{w.Start, w.End, len(w.Files), w.Limited, w.Bytes()}, false)
		if printer.Err != nil {
			cancel()
		}
	})
	if err != nil {
		return fail(err, password)
	}
	saved := options
	saved.Password = false
	saved.PasswordStdin = false
	saved.Execute = false
	saved.AckUnknown = false
	saved.Log = ""
	saved.Config = ""
	saved.Save = ""
	saved.PlanPath = ""
	saved.StatePath = ""
	plan := Plan{Version: 1, Created: time.Now().UTC(), Options: saved, Diagnosis: diagnosis}
	for _, w := range windows {
		plan.Windows = append(plan.Windows, PlannedWindow{w.Start, w.End, len(w.Files), w.Limited, w.Bytes()})
	}
	if options.Save != "" {
		if err = saveJSON(options.Save, plan, true); err != nil {
			return fail(stateError(err), password)
		}
	}
	printer.Emit("plan", plan, true)
	if printer.Err != nil {
		return 7
	}
	return 0
}

func (session *commandSession) execute(ctx context.Context, diagnosis Diagnosis) int {
	options, state, printer, fail := session.options, &session.state, session.printer, session.fail
	store, password, stopped := session.store, session.password, &session.stopped
	conn := state.Plan.Options
	var err error
	if diagnosis.ID != state.Plan.Diagnosis.ID {
		return fail(invalid("table identity changed since plan creation; create a new plan"), password)
	}
	plan := make([]Window, len(state.Plan.Windows))
	for i, w := range state.Plan.Windows {
		plan[i] = Window{Start: w.Start, End: w.End}
	}
	state.Error = nil
	state.Status = "running"
	var persistErr error
	persist := func() {
		state.Updated = time.Now().UTC()
		if e := saveJSON(options.StatePath, state, false); e != nil {
			persistErr = e
		}
	}
	persist()
	if persistErr != nil {
		return fail(stateError(persistErr), password)
	}
	runner := Runner{Store: store, Options: conn, Pause: func() bool { return stopped.Load() || persistErr != nil || printer.Err != nil }, Emit: func(p Progress) {
		state.Index = p.Index
		state.Calls = p.Calls
		if p.Window.State == "ADMIN in flight" {
			state.Status = "admin_in_flight"
			persist()
		} else if state.Status == "admin_in_flight" {
			state.Status = "observing"
			persist()
		}
		printer.Emit("progress", p, false)
	}}
	state.Index, state.Calls, err = runner.Run(ctx, diagnosis, plan, state.Index, state.Calls)
	if persistErr != nil {
		return fail(stateError(persistErr), password)
	}
	if printer.Err != nil {
		state.Status = "output_failed"
		state.Error = &CommandError{Code: "output_failed", Message: printer.Err.Error(), Exit: 7}
	} else if err != nil {
		state.Error = problem(err, password)
		state.Status = "stopped"
	} else if stopped.Load() {
		state.Status = "paused"
	} else {
		state.Status = "complete"
	}
	persist()
	if persistErr != nil {
		return fail(stateError(persistErr), password)
	}
	printer.Emit("result", state, true)
	if printer.Err != nil {
		return 7
	}
	if state.Error != nil {
		return state.Error.Exit
	}
	if stopped.Load() {
		return 130
	}
	return 0
}
func main() { os.Exit(command(os.Args[1:], os.Stdout)) }
