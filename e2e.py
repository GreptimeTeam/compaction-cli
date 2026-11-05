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

"""Isolated real-standalone CLI E2E. Requires Python and Go.

Creates only the requested new output directory and owns every started process.
This is functional testing, not a throughput benchmark.
"""
import argparse
import contextlib
import datetime
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


HERE = Path(__file__).resolve().parent


def ports():
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(4)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]


def sql(port, query):
    req = urllib.request.Request(f"http://127.0.0.1:{port}/v1/sql?db=public",
                                 data=urllib.parse.urlencode({"sql": query}).encode())
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            body = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"SQL failed: {query}: {error.read().decode()}") from error
    if body.get("error") or body.get("code", 0):
        raise RuntimeError(body)
    return body


@contextlib.contextmanager
def server(binary, root, addresses, disabled=False, auth=None, prefix=None, max_input_files=None):
    http, grpc, mysql, pg = addresses
    root.mkdir(exist_ok=True)
    config = root / "standalone.toml"
    config.write_text(f'''enable_telemetry = false
[storage]
data_home = {json.dumps(str(root / "data"))}
[logging]
dir = {json.dumps(str(root / "logs"))}
[[region_engine]]
[region_engine.mito]
num_workers = 2
max_background_compactions = 1
schedule_compaction_after_edit = false
''')
    command = [str(binary), "standalone", "start", "--config-file", str(config),
               "--http-addr", f"127.0.0.1:{http}", "--grpc-bind-addr", f"127.0.0.1:{grpc}",
               "--mysql-addr", f"127.0.0.1:{mysql}", "--postgres-addr", f"127.0.0.1:{pg}"]
    if auth:
        command += ["--user-provider", f"static_user_provider:file:{auth}"]
    if prefix:
        command = list(prefix) + command
    env = {k: v for k, v in os.environ.items() if not k.startswith(("GREPTIME_", "GREPTIMEDB_"))}
    if max_input_files is not None:
        env["GREPTIME_TWCS_MAX_INPUT_FILES"] = str(max_input_files)
    if disabled:
        env["GREPTIME_DISABLE_COMPACTION"] = "true"
    with (root / "process.log").open("ab") as output:
        proc = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                env=env, start_new_session=True)
    try:
        deadline = time.monotonic() + 180
        while True:
            if proc.poll() is not None:
                raise RuntimeError(f"standalone exited {proc.returncode}; see {root / 'process.log'}")
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{http}/health", timeout=1):
                    with socket.create_connection(("127.0.0.1", mysql), timeout=1):
                        break
            except OSError:
                if time.monotonic() > deadline:
                    raise TimeoutError("standalone health timeout")
                time.sleep(.1)
        yield proc
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGTERM)
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.wait()



def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--greptime",type=Path,required=True)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    root=args.out.resolve();root.mkdir(parents=True,exist_ok=False)
    binary=root/"greptime-compaction-cli"
    subprocess.run(["go","build","-o",str(binary),"."],cwd=HERE,check=True)
    addresses=ports()
    def cli(name,*options,expected=0,password=None):
        command=[str(binary),*options]
        proc=subprocess.run(command,input=password,text=True,capture_output=True,timeout=120)
        (root/(name+".stdout")).write_text(proc.stdout)
        (root/(name+".stderr")).write_text(proc.stderr)
        assert proc.returncode==expected,(command,proc.returncode,proc.stdout,proc.stderr)
        return proc.stdout
    connection=["--port",str(addresses[2]),"--table","compaction_bench"]
    with server(args.greptime,root/"standalone",addresses,disabled=True):
        sql(addresses[0],"CREATE TABLE compaction_bench (host STRING PRIMARY KEY, row_id BIGINT, ts TIMESTAMP(9) TIME INDEX) WITH ('compaction.type'='twcs', 'compaction.twcs.time_window'='6h')")
        sql(addresses[0],"CREATE TABLE e2e_physical (ts TIMESTAMP TIME INDEX, val DOUBLE) ENGINE=metric WITH ('physical_metric_table'='')")
        sql(addresses[0],"CREATE TABLE e2e_logical (ts TIMESTAMP TIME INDEX, val DOUBLE, host STRING PRIMARY KEY) ENGINE=metric WITH ('on_physical_table'='e2e_physical')")
        sql(addresses[0],"INSERT INTO e2e_logical (ts,val,host) VALUES ('2026-07-24T12:00:00Z',1.0,'host0')")
        sql(addresses[0],"ADMIN FLUSH_TABLE('e2e_physical')")
        sql(addresses[0],"CREATE TABLE busy_bench (host STRING PRIMARY KEY, val BIGINT, ts TIMESTAMP(9) TIME INDEX) WITH ('compaction.type'='twcs','compaction.twcs.time_window'='6h')")
        for i in range(32):
            values=",".join(f"('host{j%4}',{i*256+j},'2026-07-24T12:00:{i:02d}Z')" for j in range(256))
            sql(addresses[0],"INSERT INTO busy_bench VALUES "+values)
            sql(addresses[0],"ADMIN FLUSH_TABLE('busy_bench')")
        for i in range(17):
            ts=(datetime.datetime(2026,7,24,12,tzinfo=datetime.timezone.utc)+datetime.timedelta(minutes=i*3)).isoformat()
            sql(addresses[0],f"INSERT INTO compaction_bench VALUES ('host{i%4}',{i},'{ts}')")
            sql(addresses[0],"ADMIN FLUSH_TABLE('compaction_bench')")
        sql(addresses[0],"CREATE TABLE tied_bench (host STRING PRIMARY KEY, ts TIMESTAMP(9) TIME INDEX)")
        for i in range(6):
            sql(addresses[0],f"INSERT INTO tied_bench VALUES ('host{i}','2026-07-24T12:00:00Z')")
            sql(addresses[0],"ADMIN FLUSH_TABLE('tied_bench')")
        env={**os.environ,"COMPACTCTL_E2E_PORT":str(addresses[2])}
        with (root/"manifest-ties-test.log").open("w") as out:
            subprocess.run(["go","test","-race","-run","^TestManifestTieOrderingE2E$","-v","."],cwd=HERE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=120)
    with server(args.greptime,root/"standalone",addresses):
        env={**os.environ,"COMPACTCTL_E2E_PORT":str(addresses[2]),"COMPACTCTL_BUSY_RESULTS":str(root/"busy-results.json")}
        with (root/"busy-test.log").open("w") as out:
            subprocess.run(["go","test","-race","-run","TestBusyRejectionE2E","-v","."],cwd=HERE,env=env,stdout=out,stderr=subprocess.STDOUT,check=True,timeout=180)
        cli("tables","tables","--port",str(addresses[2]),"-o","markdown")
        cli("diagnosis","diagnose",*connection,"-o","markdown")
        logical=json.loads(cli("logical","diagnose","--port",str(addresses[2]),"--table","e2e_logical","-o","json",expected=3))
        assert logical["data"]["code"]=="logical_table"
        physical=json.loads(cli("physical","diagnose","--port",str(addresses[2]),"--table","e2e_physical","-o","json"))
        assert physical["data"]["Start"].startswith("2026-")
        plan=root/"plan.json";state=root/"state.json"
        cli("plan","plan",*connection,"--start","2026-07-24T12:00:00Z","--end","2026-07-24T13:00:00Z","--request-window","20m","--limit","8","--target-files","1","--poll","1s","--quiet","2s","--save",str(plan),"-o","markdown")
        cli("guard","run","--plan",str(plan),"--state",str(state),expected=2)
        for name,expected in [("missing-table",3),("changed-identity",2)]:
            changed=json.loads(plan.read_text())
            if name=="missing-table":
                changed["options"]["Table"]=changed["diagnosis"]["Table"]="does_not_exist"
            else:
                changed["diagnosis"]["ID"]+=1
            changed_plan=root/(name+".json");changed_plan.write_text(json.dumps(changed))
            changed_state=root/(name+"-state.json")
            cli(name,"run","--plan",str(changed_plan),"--state",str(changed_state),"--execute",expected=expected)
            assert not changed_state.exists(),name
        command=[str(binary),"run","--plan",str(plan),"--state",str(state),"--execute","--log",str(root/"queries.jsonl")]
        proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        lines=[];paused=False
        for line in proc.stdout:
            lines.append(line)
            event=json.loads(line)
            if event["type"]=="progress" and event["data"]["state"]=="observing" and not paused:
                proc.send_signal(signal.SIGINT);paused=True
        stderr=proc.stderr.read();code=proc.wait(timeout=15)
        (root/"run.stdout").write_text("".join(lines));(root/"run.stderr").write_text(stderr)
        assert paused and code==130,(code,stderr,lines)
        cli("status","status","--state",str(state),"-o","markdown")
        cli("resume","resume","--state",str(state),"--execute","-o","markdown")
        final=json.loads(state.read_text());assert final["status"]=="complete"
        result=sql(addresses[0],"SELECT COUNT(*) AS n,SUM(row_id) AS s FROM compaction_bench")
        assert result["output"][0]["records"]["rows"]==[[17,136]]
        (root/"data-check.json").write_text(json.dumps(result))
        cli("final-diagnosis","diagnose",*connection,"-o","markdown")
    credentials=root/"test-credentials";password="local-cli-test-password"
    credentials.write_text(f"root={password}\n");credentials.chmod(0o600)
    try:
        with server(args.greptime,root/"authenticated",addresses,auth=credentials):
            cli("auth","tables","--port",str(addresses[2]),"--password-stdin",password=password+"\n")
            cli("auth-error","tables","--port",str(addresses[2]),"--password-stdin",password="wrong\n",expected=3)
            auth_state=root/"auth-failed-state.json"
            for trial in range(2):
                cli(f"auth-run-error-{trial}","run","--plan",str(plan),"--state",str(auth_state),"--execute","--password-stdin",password="wrong\n",expected=3)
                assert not auth_state.exists()
    finally:credentials.unlink()
    assert all(password not in f.read_text() for f in root.glob("*.stdout"))
    (root/"result.json").write_text(json.dumps({"passed":True,"scope":"CLI diagnostics, plan, execute guard, signal pause, persisted resume, JSON/JSONL/Markdown, stdin authentication, logical-table handling, SQL literal round trips, failed connection/diagnosis leaves no checkpoint; row count and sum unchanged"},indent=2))
    print("CLI E2E passed:",root)


if __name__=="__main__":main()
