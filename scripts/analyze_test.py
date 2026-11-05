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

"""Semantic checks for SST shape reporting and package argument validation."""
import json
import os
import re
import tempfile
from unittest.mock import patch
import sys
from pathlib import Path
import subprocess
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import analyze_run
import large_e2e

class ShapeTests(unittest.TestCase):
    def test_overlaps_are_region_scoped_and_capped_results_stay_sampled(self):
        def file(region,a,b,size):
            return {"region_id":region,"min_ns":a,"max_ns":b,"file_size":size,"num_rows":10,"level":1}
        hour=3600*10**9
        data={"complete":False,"files":[file(1,0,2*hour,100),file(1,hour,7*hour,70000),file(2,0,hour,200)]}
        result=analyze_run.shape(data)
        self.assertFalse(result["complete"])
        self.assertEqual(result["files_overlapping_an_earlier_file_same_region"],1)
        self.assertEqual(result["files_crossing_6h_boundaries"],1)
        self.assertEqual(result["files_under_64KiB"],2)
        self.assertEqual(result["rows_per_sst"]["sum"],30)
    def test_build_identifier_cannot_be_an_extra_flag(self):
        result=subprocess.run([sys.executable,str(ROOT/"scripts/build.py"),"--version","v1 -X bad"],capture_output=True,text=True)
        self.assertEqual(result.returncode,2)
        self.assertIn("plain build identifiers",result.stderr)

class EvidenceTests(unittest.TestCase):
    def test_duration_units(self):
        for value, expected in [("1h2m3s", 3723), ("2ms", .002), ("2µs", .000002),
                                ("2us", .000002), ("2ns", 2e-9)]:
            self.assertAlmostEqual(analyze_run.seconds(value), expected)
        for value in ("", "unknown", "2seconds", "1ms garbage"):
            with self.assertRaises(ValueError):
                analyze_run.seconds(value)

    def test_log_drift_and_year_independent_query_groups(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            logs = root / "standalone/logs"
            logs.mkdir(parents=True)
            uid = "12345678-1234-1234-1234-123456789abc"
            (logs / "greptimedb.2027-01-01").write_text(
                f"2027-01-01T00:00:00Z INFO compaction inputs: [{uid}], outputs: [{uid}], metrics: merge: 2ms, wait: 1h2m3s\n"
                "2027-01-01T00:00:01Z INFO compaction inputs: new format\n"
                "invalid INFO compaction inputs: [], outputs: [], metrics: merge: 1ms\n")
            events = [{"kind": "query-result", "value": {
                "operation": f"Scan manifest window {year}-01-01T00:00:00Z .. end",
                "elapsed": "1ms", "ok": True}} for year in (2026, 2027)]
            events.append({"kind": "query-result", "value": {"operation": "bad timing", "elapsed": "unknown", "ok": False}})
            (root / "queries.jsonl").write_text("\n".join(map(json.dumps, events)))
            result = analyze_run.analyze(root)
            self.assertEqual(result["unparsed_query_results"], [events[-1]["value"]])
            self.assertEqual(result["failed_queries"], [events[-1]["value"]])
            self.assertEqual(result["logged_phase_seconds"]["wait"]["sum"], 3723)
            self.assertEqual(result["compaction_log_lines"], 3)
            self.assertEqual(result["compaction_units"], 1)
            self.assertEqual(result["unparsed_compaction_lines"], 2)
            self.assertEqual(result["query_seconds"]["Scan manifest window"]["count"], 2)

    def test_collect_shape_preserves_table_filter_and_reports_saturation(self):
        columns = ["region_id", "file_id", "min_ns", "max_ns", "file_size", "num_rows", "num_row_groups", "level"]
        def reply(rows):
            return {"output": [{"records": {"schema": {"column_schemas": [{"name": c} for c in columns]}, "rows": rows}}]}
        with tempfile.TemporaryDirectory() as tmp, patch("analyze_run.e2e.sql") as sql, patch("analyze_run.time.sleep"):
            sql.return_value = reply([[1, "file", 0, 1, 10, 1, 1, 0]])
            data = analyze_run.collect_shape(1, 8765, 0, 10**9, Path(tmp)/"shape.json", limit=1)
            self.assertFalse(data["complete"])
            self.assertEqual(len(data["files"]), 1)
            self.assertIn("table_id = 8765", sql.call_args.args[1])
            self.assertIn("LIMIT 1", sql.call_args.args[1])
            sql.return_value = reply([])
            data = analyze_run.collect_shape(1, 8765, 0, 10**9, Path(tmp)/"shape.json")
            self.assertTrue(data["complete"])
            self.assertEqual(data["files"], [])

    def test_missing_shape_or_log_evidence_cannot_pass(self):
        valid = {"shape": {"complete": True, "sst_bytes": {"count": 1, "p50": 10}},
                 "unparsed_compaction_lines": 0, "compaction_units": 1}
        large_e2e.validate_evidence(valid, converge=True)
        for update in [{"shape": {}}, {"shape": {"complete": True, "sst_bytes": {"count": 0}}},
                       {"shape": {"complete": False}}, {"unparsed_compaction_lines": 1}, {"compaction_units": 0}]:
            with self.assertRaises(ValueError):
                large_e2e.validate_evidence(valid | update, converge=True)

    def test_bounded_run_does_not_require_completed_compaction(self):
        analysis = {"shape": {"complete": True, "sst_bytes": {"count": 1, "p50": 10}},
                    "unparsed_compaction_lines": 0, "compaction_units": 0}
        large_e2e.validate_evidence(analysis, converge=False)
        with self.assertRaisesRegex(ValueError, "completion evidence"):
            large_e2e.validate_evidence(analysis, converge=True)
        for update in ({"unparsed_compaction_lines": 1}, {"unparsed_query_results": [{}]},
                       {"shape": {"complete": False}}):
            with self.assertRaises(ValueError):
                large_e2e.validate_evidence(analysis | update, converge=False)

    def test_example_30000_ssts_fit_shape_budget(self):
        layout = large_e2e.load_layout(ROOT/"case.example.toml")
        start, step = layout["start_unix_nanos"], layout["step_nanos"]
        count = layout["sst_count"]
        columns = ["region_id", "file_id", "min_ns", "max_ns", "file_size", "num_rows", "num_row_groups", "level"]
        def sql(port, query):
            self.assertIn("table_id = 8765", query)
            a, b = map(int, re.findall(r"CAST\((\d+) AS TIMESTAMP", query))
            limit = int(re.search(r"LIMIT (\d+)", query)[1])
            first = max(0, (a-start+step-1)//step)
            end = min(count, (b-start+step-1)//step, first+limit)
            rows = [[1, str(i), start+i*step, start+i*step, 100, 1, 1, 0] for i in range(first, end)]
            return {"output": [{"records": {"schema": {"column_schemas": [{"name": c} for c in columns]}, "rows": rows}}]}
        with tempfile.TemporaryDirectory() as tmp, patch("analyze_run.e2e.sql", side_effect=sql), patch("analyze_run.time.sleep"):
            before = analyze_run.collect_shape(1, 8765, start, start+count*step, Path(tmp)/"before.json")
            after = analyze_run.collect_shape(1, 8765, start, start+count*step, Path(tmp)/"after.json", limit=1000)
            self.assertFalse(before["complete"])
            self.assertLess(len(before["files"]), count)
            self.assertTrue(after["complete"])
            self.assertEqual({f["file_id"] for f in after["files"]}, {str(i) for i in range(count)})

    def test_example_case_and_invalid_layout(self):
        self.assertEqual(large_e2e.load_layout(ROOT/"case.example.toml")["sst_count"], 30000)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"case.toml"
            original = (ROOT/"case.example.toml").read_text()
            for before, after in [("regions = 1", "regions = 2"), ("step_nanos = 1000000000", "step_nanos = 0")]:
                path.write_text(original.replace(before, after))
                with self.assertRaises(ValueError):
                    large_e2e.load_layout(path)

class ReleaseTests(unittest.TestCase):
    def test_publish_first_run_retry_and_upload_failure(self):
        # Run the actual workflow shell with a local gh stub; no remote writes.
        workflow = (ROOT/".github/workflows/ci.yml").read_text()
        script = workflow.split('          args=(--verify-tag --generate-notes)', 1)[1]
        script = 'args=(--verify-tag --generate-notes)' + script
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            gh = root/"gh"
            gh.write_text("#!/bin/sh\necho \"$*\" >> \"$CALLS\"\n"
                          'case "$2" in\nview) exit "$EXISTS";;\nupload) exit "$UPLOAD";;\nesac\n')
            gh.chmod(0o700)
            for exists, upload, expected in [(1, 0, 0), (0, 0, 0), (0, 1, 1)]:
                calls = root/"calls"
                calls.write_text("")
                env = dict(os.environ, PATH=str(root)+os.pathsep+os.environ["PATH"],
                           CALLS=str(calls), TAG="v0.1.0-rc1", EXISTS=str(exists), UPLOAD=str(upload))
                result = subprocess.run(["bash", "-e", "-c", script], env=env, cwd=root, capture_output=True)
                self.assertEqual(result.returncode, expected, result.stderr)
                commands = calls.read_text()
                self.assertEqual("release create" in commands, exists == 1)
                self.assertIn("release upload v0.1.0-rc1 dist/* --clobber", commands)
                if exists:
                    self.assertIn("--prerelease", commands)

if __name__=="__main__":unittest.main()
