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

"""Build a portable CLI archive with documentation, skill, and SHA-256."""
import argparse
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile

ROOT=Path(__file__).resolve().parents[1]

def main():
    p=argparse.ArgumentParser()
    p.add_argument("--os",choices=["linux","darwin"],default="linux")
    p.add_argument("--arch",choices=["amd64","arm64"],default="amd64")
    p.add_argument("--version",default="dev")
    p.add_argument("--commit",default="unknown")
    p.add_argument("--out",type=Path,default=ROOT/"dist")
    args=p.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.+-]+",args.version) or not re.fullmatch(r"[A-Za-z0-9_.+-]+",args.commit):
        p.error("version/commit must be plain build identifiers")
    args.out.mkdir(parents=True,exist_ok=True)
    name=f"greptime-compaction-cli-{args.version}-{args.os}-{args.arch}"
    with tempfile.TemporaryDirectory() as work:
        stage=Path(work)/name;stage.mkdir()
        env={**os.environ,"CGO_ENABLED":"0","GOOS":args.os,"GOARCH":args.arch}
        subprocess.run(["go","build","-trimpath","-ldflags",f"-s -w -X main.version={args.version} -X main.commit={args.commit}","-o",str(stage/"greptime-compaction-cli"),"."],cwd=ROOT,env=env,check=True)
        for file in ("LICENSE","README.md","AGENTS.md","config.example.toml"):shutil.copy2(ROOT/file,stage/file)
        shutil.copytree(ROOT/"skills",stage/"skills")
        shutil.copytree(ROOT/"docs",stage/"docs")
        archive=args.out/(name+".tar.gz")
        with tarfile.open(archive,"w:gz") as out:out.add(stage,arcname=name)
    archive.with_name(archive.name+".sha256").write_text(hashlib.sha256(archive.read_bytes()).hexdigest()+"  "+archive.name+"\n")
    print(archive)

if __name__=="__main__":main()
