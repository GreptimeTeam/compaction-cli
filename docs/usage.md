# Command reference

## Commands

| Command | Purpose | Submits compaction |
|---|---|---|
| `tables` | First 200 table names, IDs and engines | No |
| `diagnose --table NAME` | SST/manifest statistics, time bounds and TWCS window | No |
| `plan --table NAME --start ... --end ... [--save FILE]` | Scan a range and generate windows | No |
| `run --plan FILE --state FILE --execute` | Execute a saved plan | Yes |
| `status --state FILE` | Read the last checkpoint; no database connection | No |
| `resume --state FILE --execute` | Recheck and continue the saved execution | Yes |
| `skill` | Print the bundled agent skill | No |
| `version` / `--version` | Print build version and commit | No |

Put flags after the command. Explicit flags override the
[TOML configuration](../config.example.toml). Unknown keys are errors.
`run` and `resume` use connection, range and tuning settings saved in the plan;
create another plan to change those settings. Command-specific flags and TOML
keys are rejected outside their commands; `--ack-unknown` is resume-only. They accept state, auth, output,
execution and audit-log flags. Existing plan, initial state and audit files are
not overwritten.

## Authentication

Set `host`, `port`, `user` and `database` in TOML or flags. The default port is
4002. Without a password flag, an authentication rejection prompts once when
stdin is a terminal. `-p` requests a hidden prompt immediately.

For automation, use `--password-stdin`. Supply the secret through stdin and close
the stream. The CLI strips one trailing newline and accepts at most 64 KiB.
Passwords are not accepted in config or flag values and are not stored in
plans/state/logs. Supply credentials again on resume.

The current connection does not configure TLS. Use a trusted network or an
authenticated tunnel when transport protection is required.

## Statistics

Diagnosis reads `region_statistics`, two ordered manifest `LIMIT 1` probes, and
the first region's TWCS option. It rejects duplicate region IDs or more than 256
regions. Missing statistics are reported as unavailable; empty tables report no
SSTs. Statistics may lag recent storage changes. Region timestamp fields remain in
diagnosis JSON and saved plans. File timestamp CASTs are requested only when
`--log` enables detailed manifest audit snapshots.

Logical metric tables return `logical_table` with a `physical_table` field.
Selecting that physical table includes its shared storage. The CLI does not
switch automatically. Physical metric totals include metadata SSTs; time-range
scans exclude them with `region_group = 0`.

A range assigns files by `max_ts`: start inclusive, end exclusive. It is not an
intersection query for every file touching the range. Every manifest query has
a table filter and LIMIT. Planning covers the range before splitting dense
windows. When the probe budget is too small, initial windows are widened; inspect
the actual plan bounds.

A capped count (`>=N` in Markdown, `limited: true` in JSON) is a lower bound.
Observed bytes and average size describe the returned files. A capped average
is neither an exact whole-window average nor a lower bound. Separate probes do
not form a transactionally consistent snapshot while files are changing.

## Output

`-o` and `--output` accept:

| Format | Behavior |
|---|---|
| `jsonl` (default) | One event per line, including scan and progress events |
| `json` | One final result; no intermediate events |
| `markdown` | Tables, progress rows and final status |

JSON envelopes contain `version`, `type`, `time` and `data`. Types are `tables`,
`diagnosis`, `scan`, `plan`, `progress`, `status`, `result` and `error`.
Early failures emit `error`; execution failures emit `result` with `data.error`.
Check both the exit status and final result. Help, version and skill commands
produce plain text. Prompts and flag help go to stderr.

Progress fields:

| Field | Meaning |
|---|---|
| `window_index` | Zero-based current window |
| `admin_attempts` | Submitted attempts, including rejected/uncertain requests |
| `start`, `end` | Current window bounds |
| `sst_count`, `limited` | Observed count and whether it is capped |
| `observed_bytes` | Bytes in the returned sample |
| `table_sst_count` | Whole-table count, or null if not refreshed in this event |
| `state`, `message` | Current operation and explanation |

State/result fields include `status`, `next_window`, `admin_attempts`, `plan` and
optional `error`. `next_window` is the zero-based index to recheck on resume;
a checkpoint may repeat a window that already met its target. It equals the
window count on completion. Markdown reports checkpointed windows, not a live
completion estimate. ADMIN attempts are not background merge units.

Plan/state schema version is 1. The version is checked before strict decoding.
Malformed documents, unsupported versions and unknown fields return exit 2;
file access and write failures return exit 7. Some nested options/diagnosis fields retain Go
names (`Start`, `End`, `Regions`). Keep these files intact; use the lower-case
window/progress fields for rendering rather than rewriting execution artifacts.

## Tuning

| Flag / TOML key | Default | Limit or purpose |
|---|---:|---|
| `limit` | 1000 | Manifest rows per query, 2–1000 |
| `scan-budget` | 1024 | Probes per plan, 1–4096 |
| `scan-interval` | 500ms | Delay between probes, at least 100ms |
| `request-window` | 1h | Initial planning interval |
| `min-window` | 1m | Smallest adaptive interval, at least 1s |
| `target-files` | 50 | Exact count target per interval, below LIMIT |
| `max-calls` | 100 | ADMIN attempt budget across run/resume |
| `query-timeout` | 10s | Per-read client deadline |
| `admin-timeout` | 5m | ADMIN response deadline |
| `poll` | 2s | Observation interval, at least 1s |
| `quiet` | 10s | Unchanged observations required before proceeding |
| `observe-timeout` | 2m | Observation budget after an ADMIN |
| `log` | off | New mode-0600 JSONL audit file |

The default planning budget allows up to 1,024 probes of 1,000 rows each.
Probes can overlap, so returned rows are not a distinct-file count. Using the
full budget adds about 8.5 minutes of pacing plus query time. Reduce the budget
or narrow the range when a shorter preview is needed.

Reads and ADMIN use one connection. Requests use `parallelism=1`.
`GREPTIME_TWCS_MAX_INPUT_FILES` is a server setting, not a CLI option. Input caps
can differ between compaction strategies and versions.

A short request interval does not confine server work to that interval. TWCS may
include other files in its table-defined window, then continue automatically
elsewhere in the region. Table-level ADMIN applies to all table regions. LIMIT
bounds returned rows; client timeouts do not guarantee server-side cancellation.

## Recovery

SIGINT/SIGTERM stops future submissions, waits for the current SQL response or
deadline, saves state and exits 130. Accepted server work continues. Read-only
scans cancel their client context.

An initial state is created only after connection, diagnosis and table identity
checks succeed. Those failures leave no checkpoint; retry `run` directly. The
`.lock` file may remain and is reusable. State is persisted before ADMIN
submission and replaced atomically. File data
and the parent directory are synced; a sync failure stops submissions. A file lock
prevents two writers using the same state file. Different state files and other
clients can still conflict on the same table. An audit write failure stops
further SQL submissions.

| Error | Exit | Next step |
|---|---:|---|
| `invalid_arguments` | 2 | Correct the arguments/config/plan |
| `connection_failed`, `query_failed`, `logical_table` | 3 | Check connectivity, permissions, schema or physical-table scope |
| `admin_busy` | 4 | Observe existing work, then resume after the poll cooldown |
| `admin_unknown`, `acknowledgement_required` | 5 | Inspect server progress before deciding whether to resume with `--ack-unknown` |
| `observation_timeout`, `no_progress`, `budget_exhausted` | 6 | Inspect progress and settings; do not reset budgets in a retry loop |
| `state_io`, `audit_failed`, `output_failed` | 7 | Repair persistence/output and retain the state |
| `interrupted` / paused | 130 | Resume when appropriate |

There is no automatic ADMIN retry. Busy classification requires the known MySQL
error number and rejection message for a single region. Multi-region errors can
be partial failures and remain uncertain. A crash in `admin_in_flight` also
requires acknowledgement before resuming. Elapsed time alone does not establish
that the server finished or rejected a request.

`status` is an offline checkpoint. Use diagnosis/planning for fresh observations.
Stable samples are not proof that the scheduler is idle. Concurrent writes can
prevent stability or add files to previously completed windows.

## Docker

The image supports Linux amd64/arm64 and defaults to UID 65532. Mount a writable
working directory. For a local Linux MySQL endpoint:

```sh
docker run --rm --network host --user "$(id -u):$(id -g)" \
  -v "$PWD:/work" ghcr.io/greptimeteam/compaction-cli:<published-version> \
  diagnose --config /work/customer.toml --table logs -o markdown
```

Replace `<published-version>` with an existing tag. Add Docker `-i` for password
stdin. The scratch image has no shell. Container `127.0.0.1` is not the host's
loopback unless networking is configured accordingly.

## Development, CI and validation

```sh
make check
make lint
make build
python3 e2e.py --greptime /absolute/path/to/greptime --out /tmp/new-cli-e2e
python3 large_e2e.py --greptime /path/to/greptime \
  --snapshot /path/to/immutable/snapshot --case /path/to/case.toml \
  --out /path/to/new-result --converge
```

Small E2E uses isolated standalone instances and real MySQL. It covers output
formats, password stdin, logical tables, pause/resume and concurrent ADMIN
rejection. Running unit tests without a server does not cover these cases.

Large E2E requires Linux cgroup v2 and a systemd user manager. Defaults are
30,000 SSTs, one region, 1 CPU, 1 GiB and no swap. It copies the fixture, records
CPU/cgroup memory and logs, checks boundary rows, and collects final SST shape.
`--converge` uses a 30-minute observation budget; `--max-input-files` changes only
the isolated test server. Tiny synthetic files do not reproduce production I/O
volume. Cgroup memory includes page cache. Interior rows are not fully checked.

Lint checks naming, documentation, error handling, SQL cleanup and complexity.
Use the tool version pinned in CI; see [Go review guidance](../AGENTS.md).

CI uses standard GitHub runners for lint, tests, four-platform archives and a
two-platform Linux image. Branch pushes, PRs and manual workflow runs only run
checks. Pushing a `v*` version tag builds and publishes via scoped
`GITHUB_TOKEN` permissions; prereleases do not update `latest`. Artifacts expire
after seven days. GHCR visibility may need adjustment after first publication.

Binary archives support Linux and macOS on amd64/arm64. Windows is not currently
supported; the state lock uses Unix APIs.

Real-server tests remain explicit local commands until CI has a pinned compatible
server artifact and fixture. Keep the large comparison suite separate from PR
checks. The currently tested main revision is
`351f4207b4446e48befc02b03779849013fc2886`; older servers require separate validation,
particularly for manifest LIMIT pushdown and compaction behavior.
