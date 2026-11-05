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

// Command greptime-compaction-cli inspects SSTs and runs bounded compaction plans.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/term"
)

// Options holds validated connection, planning and execution settings.
type Options struct {
	Provided                              map[string]bool `json:"-"`
	Output                                string          // Kept in schema v1; run/resume use invocation output flags.
	Start, End, Save, PlanPath, StatePath string
	Execute, AckUnknown, PasswordStdin    bool

	Config                                      string
	ScanBudget                                  int
	MinWindow, RequestWindow, ScanInterval      time.Duration
	Host, User, Database, Table, Log            string
	Port, Limit, Target, MaxCalls               int
	Password                                    bool
	Timeout, AdminTimeout, Poll, Quiet, Observe time.Duration
}

func parseOptions(args []string) (Options, error) {
	fs := flag.NewFlagSet("greptime-compaction-cli", flag.ContinueOnError)
	var options Options
	fs.StringVar(&options.Output, "o", "jsonl", "Output format: jsonl, json, markdown")
	fs.StringVar(&options.Output, "output", "jsonl", "Output format: jsonl, json, markdown")
	fs.StringVar(&options.Start, "start", "", "Inclusive RFC3339 start (plan)")
	fs.StringVar(&options.End, "end", "", "Exclusive RFC3339 end (plan)")
	fs.StringVar(&options.Save, "save", "", "New plan JSON file (plan)")
	fs.StringVar(&options.PlanPath, "plan", "", "Saved plan JSON file (run)")
	fs.StringVar(&options.StatePath, "state", "", "Execution state JSON file (run/resume/status)")
	fs.BoolVar(&options.Execute, "execute", false, "Explicitly authorize ADMIN submissions")
	fs.BoolVar(&options.AckUnknown, "ack-unknown", false, "Acknowledge previous ADMIN may have been accepted (resume)")
	fs.BoolVar(&options.PasswordStdin, "password-stdin", false, "Read password from stdin; no terminal required")
	fs.StringVar(&options.Host, "host", "127.0.0.1", "GreptimeDB MySQL host")
	fs.IntVar(&options.Port, "port", 4002, "MySQL port")
	fs.StringVar(&options.User, "user", "root", "MySQL username")
	fs.StringVar(&options.Database, "database", "public", "Database/schema")
	fs.StringVar(&options.Table, "table", "", "Target table name")
	fs.BoolVar(&options.Password, "password", false, "Prompt for password without echo (no value)")
	fs.BoolVar(&options.Password, "p", false, "Prompt for password without echo")
	fs.IntVar(&options.Limit, "limit", 1000, "Maximum returned rows per manifest query (2..1000)")
	fs.IntVar(&options.Target, "target-files", 50, "Stop a request window when its exact SST count is at most this")
	fs.IntVar(&options.MaxCalls, "max-calls", 100, "Maximum ADMIN submissions per run")
	fs.DurationVar(&options.Timeout, "query-timeout", 10*time.Second, "Read query timeout")
	fs.DurationVar(&options.AdminTimeout, "admin-timeout", 5*time.Minute, "ADMIN response timeout; never automatically retried")
	fs.DurationVar(&options.Poll, "poll", 2*time.Second, "Serial observation query interval (at least 1s)")
	fs.DurationVar(&options.Quiet, "quiet", 10*time.Second, "Required unchanged observations before another ADMIN")
	fs.DurationVar(&options.Observe, "observe-timeout", 2*time.Minute, "Maximum observation time after each ADMIN")
	fs.StringVar(&options.Log, "log", "", "Optional JSONL query/action audit path; must not exist")
	fs.StringVar(&options.Config, "config", "", "TOML configuration file; explicit CLI flags override file values")
	fs.IntVar(&options.ScanBudget, "scan-budget", 1024, "Maximum manifest queries per preview (1..4096)")
	fs.DurationVar(&options.MinWindow, "min-window", time.Minute, "Minimum adaptive request window (at least 1s)")
	fs.DurationVar(&options.RequestWindow, "request-window", time.Hour, "Initial planning window duration")
	fs.DurationVar(&options.ScanInterval, "scan-interval", 500*time.Millisecond, "Delay between scan queries (at least 100ms)")
	if err := fs.Parse(args); err != nil {
		return options, err
	}
	if fs.NArg() != 0 {
		return options, fmt.Errorf("unexpected arguments: use named flags; passwords must be entered at the prompt")
	}
	explicit := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { explicit[f.Name] = true })
	options.Provided = explicit
	if explicit["o"] || explicit["output"] {
		explicit["o"] = true
		explicit["output"] = true
	}
	if options.Config != "" {
		if err := loadConfig(fs, options.Config, explicit); err != nil {
			return options, err
		}
	}
	return options, options.Validate()
}
func loadConfig(fs *flag.FlagSet, path string, explicit map[string]bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()
	body, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}
	if len(body) > 1<<20 {
		return fmt.Errorf("config exceeds 1 MiB")
	}
	var values map[string]any
	if err = toml.Unmarshal(body, &values); err != nil {
		return fmt.Errorf("parse config %s: %w", path, err)
	}
	for key, value := range values {
		if key == "password" || key == "p" || key == "config" || key == "execute" || key == "ack-unknown" || key == "password-stdin" {
			return fmt.Errorf("config key %q is not allowed; use -p for an interactive password prompt", key)
		}
		if fs.Lookup(key) == nil {
			return fmt.Errorf("unknown config key %q; see --help for supported names", key)
		}
		var text string
		switch v := value.(type) {
		case string:
			text = v
		case int64:
			text = strconv.FormatInt(v, 10)
		default:
			return fmt.Errorf("config key %q must be a string or integer", key)
		}
		if !explicit[key] {
			if err = fs.Set(key, text); err != nil {
				return fmt.Errorf("config key %q: %w", key, err)
			}
			explicit[key] = true
		}
	}
	return nil
}

// Validate checks option bounds and relationships before any database access.
func (o Options) Validate() error {
	if o.Output != "jsonl" && o.Output != "json" && o.Output != "markdown" {
		return fmt.Errorf("output must be jsonl, json, or markdown")
	}
	if o.Password && o.PasswordStdin {
		return fmt.Errorf("choose -p or --password-stdin")
	}

	if err := o.validateScan(); err != nil {
		return err
	}
	if o.Port < 1 || o.Port > 65535 {
		return fmt.Errorf("--port must be 1..65535")
	}
	if o.MaxCalls < 1 || o.Timeout <= 0 || o.AdminTimeout <= 0 {
		return fmt.Errorf("timeouts and --max-calls must be positive")
	}
	if o.Poll < time.Second || o.Quiet < o.Poll || o.Observe < o.Quiet {
		return fmt.Errorf("require --poll >= 1s, --quiet >= --poll, and --observe-timeout >= --quiet")
	}
	if o.Database == "" || o.Host == "" {
		return fmt.Errorf("host and database must not be empty")
	}
	return nil
}
func (o Options) validateScan() error {
	if o.Limit < 2 || o.Limit > 1000 {
		return fmt.Errorf("--limit must be 2..1000; manifest scans are always bounded")
	}
	if o.Target < 1 || o.Target >= o.Limit {
		return fmt.Errorf("--target-files must be positive and less than --limit")
	}
	if o.ScanBudget < 1 || o.ScanBudget > 4096 {
		return fmt.Errorf("scan-budget must be 1..4096")
	}
	if o.MinWindow < time.Second || o.RequestWindow < o.MinWindow {
		return fmt.Errorf("require min-window >= 1s and request-window >= min-window")
	}
	if o.ScanInterval < 100*time.Millisecond {
		return fmt.Errorf("scan-interval must be at least 100ms")
	}
	return nil
}

func (o Options) scanConfig() ScanConfig {
	return ScanConfig{Budget: o.ScanBudget, RequestWindow: o.RequestWindow, MinWindow: o.MinWindow, Interval: o.ScanInterval}
}

func (o Options) runConfig() RunConfig {
	return RunConfig{Target: o.Target, MaxCalls: o.MaxCalls, Poll: o.Poll, Quiet: o.Quiet, Observe: o.Observe, AdminTimeout: o.AdminTimeout}
}

func (o Options) planRange() (time.Time, time.Time, error) {
	if o.Start == "" || o.End == "" {
		return time.Time{}, time.Time{}, invalid("plan requires explicit --start and --end; use diagnose to discover bounds")
	}
	start, err := time.Parse(time.RFC3339Nano, o.Start)
	if err != nil {
		return time.Time{}, time.Time{}, invalid("invalid --start RFC3339")
	}
	end, err := time.Parse(time.RFC3339Nano, o.End)
	if err != nil {
		return time.Time{}, time.Time{}, invalid("invalid --end RFC3339")
	}
	return start, end, validateRange(start, end)
}

func validateCommand(cmd string, options Options) error {
	switch cmd {
	case "tables", "diagnose", "plan", "run", "resume", "status":
	default:
		return invalid("unknown command: " + cmd)
	}
	commands := map[string]string{
		"start": "plan", "end": "plan", "save": "plan", "plan": "run",
		"state": "run resume status", "execute": "run resume", "ack-unknown": "resume",
		"table": "diagnose plan",
		"limit": "plan", "target-files": "plan", "max-calls": "plan",
		"admin-timeout": "plan", "poll": "plan", "quiet": "plan", "observe-timeout": "plan",
		"scan-budget": "plan", "min-window": "plan", "request-window": "plan", "scan-interval": "plan",
		"host": "tables diagnose plan", "port": "tables diagnose plan",
		"user": "tables diagnose plan", "database": "tables diagnose plan", "query-timeout": "tables diagnose plan",
		"password": "tables diagnose plan run resume", "p": "tables diagnose plan run resume",
		"password-stdin": "tables diagnose plan run resume", "log": "tables diagnose plan run resume",
		"config": "tables diagnose plan",
	}
	for key, allowed := range commands {
		if options.Provided[key] && !slices.Contains(strings.Fields(allowed), cmd) {
			return invalid("--" + key + " is not supported by " + cmd)
		}
	}
	if cmd == "plan" {
		_, _, err := options.planRange()
		return err
	}
	return nil
}

func readPassword() (string, error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return "", fmt.Errorf("password required: use --password-stdin for agents, or an interactive terminal with -p")
	}
	fmt.Fprint(os.Stderr, "MySQL password: ")
	b, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(os.Stderr)
	if err != nil {
		return "", fmt.Errorf("read password: %w", err)
	}
	return string(b), nil
}
func connect(o Options, password string) (*sql.DB, error) {
	config := mysql.NewConfig()
	config.User = o.User
	config.Passwd = password
	config.Net = "tcp"
	config.Addr = net.JoinHostPort(o.Host, strconv.Itoa(o.Port))
	config.DBName = o.Database
	config.Timeout = o.Timeout
	// Use text queries and UTC timestamp literals. Never emit the formatted DSN.
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()
	if err = db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
func friendly(err error, password string) string {
	msg := err.Error()
	if password != "" {
		msg = strings.ReplaceAll(msg, password, "[redacted]")
	}
	var authErr *mysql.MySQLError
	switch {
	case errors.As(err, &authErr) && (authErr.Number == 1045 || authErr.Number == 1698):
		msg += "; authentication rejected: check username/password and the server user-provider configuration"
	case errors.Is(err, context.DeadlineExceeded):
		msg += "; query timed out. Narrow the range or lower LIMIT, then retry the diagnostic"
	default:
		var ne net.Error
		if errors.As(err, &ne) {
			msg += "; check MySQL host/port, network reachability, and whether GreptimeDB is running"
		}
	}
	return msg
}
