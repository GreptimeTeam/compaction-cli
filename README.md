# compaction-cli

Inspect GreptimeDB SST files and compact a selected time range. Output is JSONL,
JSON, or Markdown. The tool connects through the MySQL endpoint.

## Install

Download a binary from [Releases](https://github.com/GreptimeTeam/compaction-cli/releases),
or build with Go 1.24+:

```sh
go build -o greptime-compaction-cli .
```

Copy [config.example.toml](config.example.toml) and set your host, port, user and
database. Use `-p` for a hidden password prompt, or `--password-stdin` for agents.
Passwords do not belong in config files or command arguments.

## Common tasks

### Check a table

```sh
greptime-compaction-cli tables --config customer.toml -o markdown
greptime-compaction-cli diagnose --config customer.toml --table logs -o markdown
```

Shows SST count, total and average size, manifest size, time bounds and TWCS
window. Logical metric tables report their shared physical table; selection is
explicit.

### Check SSTs in a time range

```sh
greptime-compaction-cli plan --config customer.toml --table logs \
  --start 2026-07-24T14:00:00Z --end 2026-07-24T18:00:00Z \
  --request-window 30m -o markdown
```

This is read-only. Each row shows a window's file count, observed bytes and
average size. `>=` means the query hit its limit; the average then describes only
the returned files. Start is inclusive, end exclusive. Files are assigned by
`max_ts`.

### Compact that range

Add `--save plan.json` to the previous command, review the plan, then run:

```sh
greptime-compaction-cli run --plan plan.json --state run.json --execute -o jsonl
```

Execution rechecks each window and submits serial ADMIN requests. Connection
and tuning settings come from the saved plan. A small request window does not
prevent GreptimeDB from continuing compaction elsewhere in the region.

### Pause and resume

Press Ctrl-C to stop further submissions. Already accepted server work continues.

```sh
greptime-compaction-cli status --state run.json -o markdown
greptime-compaction-cli resume --state run.json --execute -o jsonl
```

Busy, uncertain, and failed ADMIN requests are not retried automatically. Inspect
progress before resuming; an unknown outcome also requires `--ack-unknown`.

## For agents

Install the [bundled skill](skills/compaction-cli/SKILL.md):

```sh
mkdir -p ~/.agents/skills/compaction-cli
greptime-compaction-cli skill > ~/.agents/skills/compaction-cli/SKILL.md
```

Example requests:

- “Show the SST count and average size for `logs` between 14:00 and 18:00 UTC.”
- “Plan compaction for that range. Show the windows before executing.”
- “Execute this plan and report file reduction, ADMIN attempts and failures.”

Use `-o jsonl` for progress, `-o json` for one final result, or `-o markdown` for
readable tables. Preserve the state file when resuming. See the
[command and error reference](docs/usage.md) for the output contract and limits.
For ongoing cleanup across all regions, use the [goal workflow](docs/goal-mode.md).

## Docker and CI

Images: `ghcr.io/greptimeteam/compaction-cli:<version>`. Mount a writable `/work`
directory for configuration, plans and state; use `-i` with `--password-stdin`.

CI checks formatting, Go lint, vet, race tests and dependencies. It builds Linux
and macOS amd64/arm64 binaries and a Linux amd64/arm64 image on standard GitHub
runners. PRs only build; version tags publish binaries and GHCR images.

Run `make check` locally. [Validation instructions](docs/usage.md#development-ci-and-validation)
cover real-server tests and the 30,000-SST case. Tested server behavior and
performance depend on the GreptimeDB version; manifest LIMIT pushdown is required.
