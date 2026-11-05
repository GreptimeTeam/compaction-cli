# Long-running compaction goals

Goal mode is an agent workflow using the existing CLI commands. There is no
`goal` subcommand or built-in scheduler. The agent keeps checkpoints, schedules
fresh observations, and decides whether another pass is justified.

## Example request

> Reduce small SST files for `public.logs` across all its regions. Use
> `customer.toml`. Start with existing historical data, leaving the current
> six-hour TWCS window out of manual requests. I authorize serial compaction
> within that scope. Run for up to 24 hours with at most 100 ADMIN attempts
> across all plans and resumes. Check progress every five minutes and report
> every 30 minutes. Start at 50 files per planning interval, then try lower
> targets where further merging is useful. Stop submitting on uncertain ADMIN
> outcomes, persistent lack of progress, or resource pressure. Preserve state
> so another agent can continue. Report remaining files per region and any
> ranges you could not verify.

Adjust the deadline, call budget, observation interval and resource thresholds
for the deployment. The CLI does not monitor server CPU, memory, disk space or
query latency; the agent needs access to those metrics to enforce such limits.
If those metrics are unavailable, report that limitation before execution.

## Define the scope

Run `diagnose` and save its output. Record the server and CLI versions, table ID,
region IDs, SST counts/bytes, manifest sizes, time bounds and TWCS option.
A logical table needs an explicit decision about its shared physical table.

For an initial cleanup, freeze a cutoff at a completed TWCS window boundary.
Cover the oldest observed SST through that cutoff. To include every existing
SST, the exclusive end must be later than the greatest observed `max_ts`.
Use timestamp precision correctly; do not round the maximum down. Files are
assigned by `max_ts`, so a file spanning the cutoff may be left for a later pass.

For ongoing maintenance, persist the last verified cutoff and advance it only
to newly closed windows. Periodically rescan older ranges for late writes.
Keep the original deadline and total call budget unless the user extends them.
Accepted server work can continue outside the requested interval, including
active windows; excluding recent data from a plan does not isolate server work.

## Run a pass

Use a new directory for each pass. These commands assume credentials are supplied
separately when required:

```sh
greptime-compaction-cli diagnose --config customer.toml --table logs \
  -o json > before.json
greptime-compaction-cli plan --config customer.toml --table logs \
  --start 2026-07-24T00:00:00Z --end 2026-07-31T00:00:00Z \
  --request-window 1h --target-files 50 --max-calls 20 \
  --save plan.json -o json > planned.json
greptime-compaction-cli run --plan plan.json --state state.json \
  --execute -o jsonl > events.jsonl
```

Replace the example range with the agreed bounds. Allocate each plan at most
the remaining goal budget. Keep one active execution per table, including across
agent restarts; the CLI lock only protects callers sharing one state file.

The CLI observes accepted work and rechecks each window before submitting.
Let that observation finish. An ADMIN response does not mean all background
compaction finished. On restart, read the saved state and fresh diagnostics
before resuming; the server may have continued while the CLI was stopped.

A completed pass means its windows met `target-files` when checked. It does not
mean the table has reached its minimum possible file count. Recheck the full
scope after background activity settles. If smaller files remain and resources
permit, create a new pass with a lower target, for example 50, then 10, then 2.
These are policy choices, not guaranteed attainable counts. Keep the planning
intervals comparable: splitting a window until each piece meets the target
cannot establish that files were merged.

Do not retry an unchanged plan indefinitely. After a `no_progress` result, use
fresh statistics, bounded probes and server logs to establish what happened.
Capped samples and unchanged totals alone cannot prove there is no eligible
work. If a region cannot make further progress, record the reason or the missing
evidence and leave it unresolved. Do not change table options or server input
limits without authorization covering those changes.

## All-region coverage

Table-level ADMIN targets all table regions. Current planning counts files
across regions; it has no region-selection flag or per-region target. Diagnosis
supports at most 256 regions and reads the first region's TWCS option. Check
for differing region options before assuming the same window everywhere.

Track each region separately in the goal report. Compare the region ID set on
every pass so newly created, split or moved regions are not silently omitted.
A table-wide reduction can hide a region with no improvement. Missing or
duplicate region statistics prevent an exact coverage claim.

For per-region time-range verification beyond CLI output, use separately
reviewed bounded manifest queries with table ID, region ID, time predicates and
LIMIT, spaced according to the scan budget. If that inspection is unavailable,
report per-region totals and mark range coverage unverified. Do not claim a
per-region minimum from the CLI's aggregate window targets. Physical metric
table totals also include metadata SSTs that time-range plans exclude.

## Checkpoints and stopping

Keep a small goal ledger beside the immutable plans and CLI state files:

- Endpoint/database/table ID, current region IDs, authorized scope and cutoff.
- Deadline, total ADMIN attempts across every state file, remaining budget.
- Current pass/state path, next observation time and last resource readings.
- Per-region before/after counts and bytes, capped or unverified ranges.
- Last error, recovery decision and the next action.

Resume the same state after interruption. A new plan must not reset the goal's
call count. Preserve each invocation's JSONL output in a new file. Never store
passwords in the ledger.

| Condition | Agent action |
|---|---|
| Counts are falling or server work is active | Observe; avoid another submission |
| Pass completed, useful merges may remain | Verify coverage, then lower the target within the remaining budget |
| `admin_busy` | Observe existing work and back off before resuming |
| Unknown outcome or multi-region partial failure | Reconcile affected regions; do not automatically acknowledge and resubmit |
| Observation timeout | Inspect live progress; timeout does not cancel server work |
| No progress with unchanged evidence | Stop submissions and report unresolved ranges |
| Resource threshold, deadline or call budget reached | Pause and save the ledger; accepted work may continue |
| All agreed targets verified | Finish a cleanup goal, or schedule the next maintenance check |

Stable observations are not proof of scheduler idleness or a global minimum.
Use “target met”, “no further reduction observed”, or “blocked”, with the
supporting evidence. Keep unresolved regions visible in the final report.
See [recovery](usage.md#recovery) for exit codes and acknowledgement rules.

## Progress report

Report the observation time, covered range, per-region file counts and bytes,
ADMIN attempts, server resource readings, and next action. Label sampled counts
and missing metrics. Include output size and time-span distributions when
bounded manifest inspection is available; a lower file count alone does not
show whether the resulting SSTs are useful.

ADMIN attempts and server merge units measure different things. In the
30,000-SST single-region test, the CLI submitted once, paused, then submitted
again after resume. Observed table counts after the second submission fell from
29,820 to 78. All 128 planning intervals then had at most four files, below the
configured target of 50, so the run completed. It did not stop on `no_progress`.
The 1,942 background units cannot be assigned solely to that second request,
and this test does not establish that two requests were necessary or that
78 files was the minimum.
