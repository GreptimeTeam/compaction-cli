#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Resource-limited functional CLI E2E using an immutable 30,000-SST snapshot.

Linux cgroup v2 and a working systemd user manager are required. No benchmark
throughput claim: this measures diagnostic bounds and exercises CLI behavior.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import tomllib
import uuid

import e2e
import analyze_run


def load_layout(path):
    """Validate the external snapshot's deterministic, single-region layout."""
    layout = tomllib.loads(path.read_text())["scenario"]["layout"]
    for key in ("sst_count", "regions", "rows_per_sst", "step_nanos"):
        if type(layout.get(key)) is not int or layout[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if type(layout.get("start_unix_nanos")) is not int:
        raise ValueError("start_unix_nanos must be an integer")
    if layout["regions"] != 1:
        raise ValueError("the fixture must have one region")
    if layout["sst_count"] * layout["rows_per_sst"] < 8:
        raise ValueError("the fixture must contain at least eight rows")
    return layout


def validate_evidence(analysis, *, converge):
    """Reject missing or incomplete evidence before writing a passing result."""
    summary = analysis.get("shape", {})
    if not summary.get("complete") or not summary.get("sst_bytes", {}).get("count"):
        raise ValueError("final SST shape must be complete and nonempty")
    if "p50" not in summary["sst_bytes"]:
        raise ValueError("final SST size quantiles are missing")
    if analysis["unparsed_compaction_lines"]:
        raise ValueError("server compaction log lines were not parsed")
    if analysis.get("unparsed_query_results"):
        raise ValueError("query timings were not parsed")
    if converge and not analysis["compaction_units"]:
        raise ValueError("no parsed compaction completion evidence")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--greptime", type=Path, required=True)
    p.add_argument("--snapshot", type=Path, required=True)
    p.add_argument("--case", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--converge", action="store_true")
    p.add_argument("--max-input-files", type=int)
    p.add_argument("--cpu-quota", default="100%")
    p.add_argument("--memory-max", default="1G")
    args = p.parse_args()
    root = args.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    (root / "standalone").mkdir()
    cli_binary = root / "greptime-compaction-cli"
    subprocess.run(["go", "build", "-o", str(cli_binary), "."], cwd=e2e.HERE, check=True)
    layout = load_layout(args.case)
    shutil.copyfile(args.case, root / "case.toml")
    shutil.copytree(args.snapshot, root / "standalone/data")
    (root / "snapshot-source.json").write_text(json.dumps({"source": str(args.snapshot.resolve()),
        "files": {str(f.relative_to(args.snapshot)): {"bytes": f.stat().st_size,
        "sha256": hashlib.sha256(f.read_bytes()).hexdigest()} for f in args.snapshot.rglob("*") if f.is_file()}}, indent=2))
    (root / "run.json").write_text(json.dumps(vars(args), default=str, indent=2))
    (root / "source-sha256.json").write_text(json.dumps({f.name: hashlib.sha256(f.read_bytes()).hexdigest()
        for f in e2e.HERE.iterdir() if f.suffix in (".go", ".py", ".mod", ".sum")}, indent=2))
    binary = args.greptime.resolve()
    (root / "server-version.txt").write_bytes(subprocess.check_output([str(binary), "--version"]))
    unit = f"compaction-cli-e2e-{uuid.uuid4().hex[:12]}.scope"
    prefix = ["systemd-run", "--user", "--scope", "--quiet", f"--unit={unit}",
              f"--property=CPUQuota={args.cpu_quota}", f"--property=MemoryMax={args.memory_max}",
              "--property=MemorySwapMax=0"]
    (root / "limits-command.json").write_text(json.dumps(prefix))
    addresses = e2e.ports()
    stop = threading.Event()
    try:
        with e2e.server(binary, root / "standalone", addresses, prefix=prefix, max_input_files=args.max_input_files) as proc:
            properties = subprocess.check_output(["systemctl", "--user", "show", unit,
                "-p", "ControlGroup", "-p", "MemoryMax", "-p", "MemorySwapMax", "-p", "CPUQuotaPerSecUSec"], text=True)
            (root / "limits-effective.txt").write_text(properties)
            cg = Path("/sys/fs/cgroup") / next(line.split("=", 1)[1].lstrip("/") for line in properties.splitlines() if line.startswith("ControlGroup="))
            def sample():
                with (root / "resources.jsonl").open("w") as output:
                    while not stop.is_set():
                        try:
                            row = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                   "cpu": dict(line.split() for line in (cg / "cpu.stat").read_text().splitlines()),
                                   "memory_current": int((cg / "memory.current").read_text()),
                                   "memory_peak": int((cg / "memory.peak").read_text()),
                                   "memory_events": dict(line.split() for line in (cg / "memory.events").read_text().splitlines())}
                            output.write(json.dumps(row) + "\n"); output.flush()
                        except FileNotFoundError:
                            return
                        stop.wait(.2)
            monitor = threading.Thread(target=sample)
            monitor.start()
            try:
                def cli(name, arguments, timeout=180, expected=0):
                    with (root / (name+".stdout")).open("w") as output, (root/(name+".stderr")).open("w") as error:
                        result = subprocess.run([str(cli_binary), *arguments], stdout=output, stderr=error, timeout=timeout)
                    assert result.returncode == expected, (name, result.returncode)
                connection=["--port",str(addresses[2]),"--table","compaction_bench"]
                cli("initial",["diagnose",*connection,"-o","json","--log",str(root/"initial.audit")])
                initial=json.loads((root/"initial.stdout").read_text())["data"]
                assert sum(r["Files"] for r in initial["Regions"]) == layout["sst_count"]
                plan, state = root/"plan.json", root/"state.json"
                cli("plan",["plan",*connection,"--start",initial["Start"],"--end",initial["End"],"--save",str(plan),"--limit","200","--target-files","50","--scan-interval","100ms","--poll","1s","--quiet","2s","--observe-timeout","30m","--log",str(root/"plan.audit"),"-o","markdown"])
                # Pause after the first accepted request is observable, then resume
                # from the actual persisted state in a new CLI process.
                with (root/"run.stdout").open("w") as out, (root/"run.stderr").open("w") as error:
                    child=subprocess.Popen([str(cli_binary),"run","--plan",str(plan),"--state",str(state),"--execute","--log",str(root/"run.audit")], stdout=subprocess.PIPE,stderr=error,text=True)
                    sent=False
                    try:
                        for line in child.stdout:
                            out.write(line);out.flush()
                            event=json.loads(line)
                            if event["type"]=="progress" and event["data"]["state"]=="observing" and not sent:
                                child.send_signal(signal.SIGINT);sent=True
                        assert child.wait(timeout=15)==130 and sent
                    finally:
                        if child.poll() is None: child.kill();child.wait()
                if args.converge:
                    cli("resume",["resume","--state",str(state),"--execute","--log",str(root/"resume.audit")],timeout=2400)
                    assert json.loads(state.read_text())["status"]=="complete"
                else:
                    # A bounded run tests stop/resume without asserting convergence.
                    with (root/"resume.stdout").open("w") as out, (root/"resume.stderr").open("w") as error:
                        child=subprocess.Popen([str(cli_binary),"resume","--state",str(state),"--execute","--log",str(root/"resume.audit")],stdout=out,stderr=error)
                        try:
                            try: child.wait(timeout=15)
                            except subprocess.TimeoutExpired: child.send_signal(signal.SIGINT)
                            assert child.wait(timeout=90) in (0,130)
                        finally:
                            if child.poll() is None: child.kill();child.wait()
                cli("final",["diagnose",*connection,"-o","markdown","--log",str(root/"final.audit")])
                (root/"queries.jsonl").write_text("".join((root/(name+".audit")).read_text() for name in ("initial","plan","run","resume","final")))
                # Exact deterministic boundary rows, time-pruned; no full table scan.
                n = layout["sst_count"] * layout["rows_per_sst"]
                start, step = layout["start_unix_nanos"], layout["step_nanos"]
                checks = []
                for first, end in [(0, 8), (n-8, n)]:
                    q = f"SELECT row_id, CAST(ts AS BIGINT) AS ns FROM compaction_bench WHERE ts >= CAST({start+first*step} AS TIMESTAMP(9)) AND ts < CAST({start+end*step} AS TIMESTAMP(9)) ORDER BY row_id LIMIT 8"
                    response = e2e.sql(addresses[0], q)
                    assert response["output"][0]["records"]["rows"] == [[i, start+i*step] for i in range(first,end)]
                    checks.append(response)
                (root / "boundary-checks.json").write_text(json.dumps(checks))
                analyze_run.collect_shape(addresses[0], initial["ID"], start, start+n*step, root / "final-shape.json", limit=1000)
                import urllib.request
                (root / "metrics-final.prom").write_bytes(urllib.request.urlopen(f"http://127.0.0.1:{addresses[0]}/metrics", timeout=10).read())
                assert proc.poll() is None, "server exited during the test"
                (root / "memory-events-final.txt").write_text((cg / "memory.events").read_text())
            finally:
                stop.set(); monitor.join(timeout=5)
    finally:
        subprocess.run(["systemctl", "--user", "stop", unit], capture_output=True, timeout=15)
    events = [json.loads(line) for line in (root / "queries.jsonl").read_text().splitlines()]
    queries = [e["value"]["sql"] for e in events if e["kind"] == "query"]
    manifest = [q for q in queries if "ssts_manifest" in q]
    assert all(" LIMIT " in q and "table_id = " in q for q in manifest)
    snapshots = [e["value"] for e in events if e["kind"] == "manifest-snapshot"]
    assert all(len(v["Files"] or []) <= 200 for v in snapshots)
    resources = [json.loads(line) for line in (root / "resources.jsonl").read_text().splitlines()]
    assert resources and int(resources[-1]["memory_events"]["oom_kill"]) == 0
    analysis = analyze_run.analyze(root)
    (root / "analysis.json").write_text(json.dumps(analysis, indent=2))
    validate_evidence(analysis, converge=args.converge)
    result = {"passed": True, "scope": "full plan convergence" if args.converge else "bounded CLI pause/resume, not total convergence",
              "manifest_queries": len(manifest), "max_returned_manifest_rows": max(len(v["Files"] or []) for v in snapshots),
              "cgroup_peak_bytes": max(r["memory_peak"] for r in resources),
              "cpu_seconds_during_test": (int(resources[-1]["cpu"]["usage_usec"])-int(resources[0]["cpu"]["usage_usec"]))/1e6,
              "validation": "exact first/last eight rows; interior rows not checked"}
    (root / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
