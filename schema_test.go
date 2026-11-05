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

package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

func jsonObject(t *testing.T, value any) map[string]any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(data, &object); err != nil {
		t.Fatal(err)
	}
	return object
}

func requireJSONKeys(t *testing.T, object map[string]any, names string) {
	t.Helper()
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	want := strings.Fields(names)
	slices.Sort(keys)
	slices.Sort(want)
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys=%v want=%v", keys, want)
	}
}

func TestSchemaV1Keys(t *testing.T) {
	plan := testPlan(t)
	plan.Diagnosis.Regions = []Region{{ID: 1, Min: sql.NullInt64{Int64: 1, Valid: true}}}
	planJSON := jsonObject(t, plan)
	requireJSONKeys(t, planJSON, "version created options diagnosis windows")
	requireJSONKeys(t, planJSON["options"].(map[string]any), "Output Start End Save PlanPath StatePath Execute AckUnknown PasswordStdin Config ScanBudget MinWindow RequestWindow ScanInterval Host User Database Table Log Port Limit Target MaxCalls Password Timeout AdminTimeout Poll Quiet Observe")
	diagnosis := planJSON["diagnosis"].(map[string]any)
	requireJSONKeys(t, diagnosis, "Metric Table ID Regions Start End Window")
	region := diagnosis["Regions"].([]any)[0].(map[string]any)
	requireJSONKeys(t, region, "Manifest ID Files Bytes Min Max")
	requireJSONKeys(t, region["Min"].(map[string]any), "Int64 Valid")
	requireJSONKeys(t, planJSON["windows"].([]any)[0].(map[string]any), "start end sst_count limited observed_bytes")
	state := State{Version: schemaVersion, Plan: plan, Error: &CommandError{PhysicalTable: "physical"}}
	stateJSON := jsonObject(t, state)
	requireJSONKeys(t, stateJSON, "version updated status plan next_window admin_attempts error")
	requireJSONKeys(t, stateJSON["error"].(map[string]any), "code message retryable outcome_unknown physical_table exit_code")
	state.Error = nil
	requireJSONKeys(t, jsonObject(t, state), "version updated status plan next_window admin_attempts")
	for _, format := range []string{"json", "jsonl"} {
		var output bytes.Buffer
		printer := Printer{Format: format, Out: &output}
		printer.Emit("result", state, true)
		var envelope map[string]any
		if err := json.Unmarshal(output.Bytes(), &envelope); err != nil {
			t.Fatal(err)
		}
		requireJSONKeys(t, envelope, "version type time data")
		if envelope["version"] != float64(schemaVersion) || envelope["type"] != "result" {
			t.Fatalf("envelope=%v", envelope)
		}
	}
}

func TestManifestSnapshotRetainsLegacyCalls(t *testing.T) {
	snapshot := jsonObject(t, manifestSnapshot{Window: Window{State: "scanned"}})
	requireJSONKeys(t, snapshot, "Start End Files Limited State Calls")
	if snapshot["Calls"] != float64(0) {
		t.Fatalf("Calls=%v", snapshot["Calls"])
	}
}
