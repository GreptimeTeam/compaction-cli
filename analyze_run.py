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

"""Read-only analysis of functional test artifacts; no performance attribution."""
import argparse
import collections
import datetime
import json
import math
from pathlib import Path
import re
import time

import e2e

UUID = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    return {"count": len(values), "min": values[0], "p50": values[math.ceil(.5*len(values))-1],
            "p95": values[math.ceil(.95*len(values))-1], "max": values[-1], "sum": sum(values)}


def seconds(value):
    units = {"h": 3600, "m": 60, "s": 1, "ms": .001, "µs": .000001, "us": .000001, "ns": 1e-9}
    return sum(float(n)*units[u] for n,u in re.findall(r"([\d.]+)(ns|µs|us|ms|s|m|h)", value))


def collect_shape(port, table_id, start_ns, end_ns, output, budget=128, limit=200):
    """Bounded serial max_ts scans. Saturated leaves remain explicitly sampled."""
    step = 6*3600*10**9
    pending = collections.deque((a, min(a+step,end_ns)) for a in range(start_ns,end_ns,step))
    files, probes = {}, []
    complete = True
    while pending and len(probes) < budget:
        a,b = pending.popleft()
        q = ("SELECT region_id, file_id, CAST(min_ts AS BIGINT) AS min_ns, "
             "CAST(max_ts AS BIGINT) AS max_ns, file_size, num_rows, num_row_groups, level "
             f"FROM information_schema.ssts_manifest WHERE table_id = {int(table_id)} "
             f"AND region_group = 0 AND max_ts >= CAST({a} AS TIMESTAMP(9)) "
             f"AND max_ts < CAST({b} AS TIMESTAMP(9)) ORDER BY max_ts, min_ts LIMIT {int(limit)}")
        began = time.monotonic()
        result = e2e.sql(port,q)["output"][0]["records"]
        rows = [dict(zip([c["name"] for c in result["schema"]["column_schemas"]], r)) for r in result["rows"]]
        probes.append({"sql":q,"rows":len(rows),"elapsed_seconds":time.monotonic()-began})
        if len(rows) >= limit and b-a >= 2*10**9 and len(probes)+len(pending)+2 <= budget:
            mid = (a+b)//2
            pending.extend([(a,mid),(mid,b)])
        else:
            complete = complete and len(rows) < limit
            for row in rows:
                files[(row["region_id"],row["file_id"])] = row
        time.sleep(.1)
    complete = complete and not pending
    data = {"complete":complete,"range_ns":[start_ns,end_ns],"probes":probes,"files":list(files.values())}
    output.write_text(json.dumps(data,indent=2))
    return data


def shape(data):
    files = data["files"]
    grouped = collections.defaultdict(list)
    for f in files:
        grouped[f["region_id"]].append(f)
    overlap = cross = 0
    for values in grouped.values():
        high = None
        for f in sorted(values,key=lambda f:f["min_ns"]):
            if high is not None and f["min_ns"] <= high:
                overlap += 1
            high = max(high if high is not None else f["max_ns"],f["max_ns"])
            cross += f["min_ns"]//(6*3600*10**9) != f["max_ns"]//(6*3600*10**9)
    return {"complete":data["complete"],"sst_bytes":distribution([f["file_size"] for f in files]),
            "rows_per_sst":distribution([f["num_rows"] for f in files]),
            "span_seconds":distribution([(f["max_ns"]-f["min_ns"])/1e9 for f in files]),
            "files_under_64KiB":sum(f["file_size"]<65536 for f in files),
            "files_under_1MiB":sum(f["file_size"]<1048576 for f in files),
            "files_overlapping_an_earlier_file_same_region":overlap,
            "files_crossing_6h_boundaries":cross,
            "levels":dict(collections.Counter(f["level"] for f in files)),
            "files_per_6h_max_ts":dict(sorted(collections.Counter(str(f["max_ns"]//(6*3600*10**9)) for f in files).items()))}


def analyze(root):
    inputs,outputs,intervals = [],[],[]
    generated,consumed = set(),set()
    phases = collections.defaultdict(list)
    warnings, errors = [],[]
    times = []
    logs = sorted((root/"standalone/logs").glob("greptimedb.20*"))
    for path in logs:
        for line in path.open():
            if " WARN " in line: warnings.append(line.strip())
            if " ERROR " in line: errors.append(line.strip())
            match = re.search(r"compaction inputs: \[(.*?)\], outputs: \[(.*?)\].*metrics: (.*)",line)
            if not match: continue
            ins,outs = re.findall(UUID,match[1]),re.findall(UUID,match[2])
            inputs.append(len(ins)); outputs.append(len(outs))
            consumed.update(ins); generated.update(outs)
            times.append(datetime.datetime.fromisoformat(line.split()[0].replace("Z","+00:00")))
            for name,duration in re.findall(r"(\w+): ([\d.]+(?:ns|µs|us|ms|s))",match[3]):
                phases[name].append(seconds(duration))
    intervals = [(b-a).total_seconds() for a,b in zip(times,times[1:])]
    audit = root/"queries.jsonl"
    if not audit.exists(): audit=root/"screens/queries.jsonl"
    events = [json.loads(l) for l in audit.open()]
    query_groups=collections.defaultdict(list)
    failed=[]
    for event in events:
        if event["kind"] == "query-result":
            value=event["value"]
            name=value["operation"].split(" 2026-")[0]
            query_groups[name].append(seconds(value["elapsed"]))
            if not value["ok"]: failed.append(value)
    result={"scope":"Observed functional test; inter-completion intervals are not task duration or causal attribution",
        "admin_submissions":sum(e["kind"]=="admin-submit" for e in events),
        "compaction_units":len(inputs),"inputs_per_unit":distribution(inputs),"outputs_per_unit":distribution(outputs),
        "outputs_later_reconsumed":len(generated & consumed),"terminal_output_ids_count":len(generated-consumed),
        "logged_phase_seconds":{k:distribution(v) for k,v in phases.items()},
        "completion_interval_seconds":distribution(intervals),
        "query_seconds":{k:distribution(v) for k,v in query_groups.items()},
        "failed_queries":failed,"server_warning_lines":warnings,"server_error_lines":errors,
        "server_log_bytes":sum(p.stat().st_size for p in logs)}
    resources_path=root/"resources.jsonl"
    if resources_path.exists():
        resources=[json.loads(l) for l in resources_path.open()]
        first,last=resources[0],resources[-1]
        result["resources"]={"peak_cgroup_bytes":max(r["memory_peak"] for r in resources),
            "cpu_seconds":(int(last["cpu"]["usage_usec"])-int(first["cpu"]["usage_usec"]))/1e6,
            "throttled_periods":int(last["cpu"]["nr_throttled"])-int(first["cpu"]["nr_throttled"]),
            "periods":int(last["cpu"]["nr_periods"])-int(first["cpu"]["nr_periods"]),
            "memory_events":last["memory_events"]}
    metrics=root/"metrics-final.prom"
    if metrics.exists():
        result["compaction_metrics"]={}
        for line in metrics.read_text().splitlines():
            if line.startswith(("greptime_mito_compaction", "greptime_mito_twcs")) and "_bucket" not in line:
                key,value=line.rsplit(" ",1)
                result["compaction_metrics"][key]=float(value)
    if (root/"final-shape.json").exists(): result["shape"]=shape(json.loads((root/"final-shape.json").read_text()))
    return result


if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--run",type=Path,required=True)
    args=parser.parse_args()
    print(json.dumps(analyze(args.run),indent=2))
