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
import sys
from pathlib import Path
import subprocess
import unittest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import analyze_run

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

if __name__=="__main__":unittest.main()
