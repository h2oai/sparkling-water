#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Runs the test scripts of one suite (SUITE=scoring|training, default scoring) as notebook tasks of a Databricks job run,
# RUNS times in a row, and records phase timings: in-script phases from the notebook exit value, platform phases from
# the Jobs API. Writes one JSON per run and their aggregate (median, min, max) to build/databricks-runner/.
# Usage: run_databricks.sh [--dry-run]   (--dry-run prints the CLI calls and the job spec, contacts nothing)
# Uses the logged-in databricks CLI (select a workspace with DATABRICKS_CONFIG_PROFILE). Parameters, never committed:
#   UPLOAD_PATH   Unity Catalog volume directory for fixtures and outputs, e.g. /Volumes/<catalog>/<schema>/<volume>/tests
#   CLUSTER_ID    existing dedicated-access cluster; unset = a new job cluster per task (SPARK_VERSION, NODE_TYPE, WORKERS)
#   WORKERS       workers per new cluster (scoring 1, training 3); H2O_NODES expected H2O nodes (default WORKERS)
#   JAVA_OPTIONS  driver and executor JVM flags of a new cluster; training default: the --add-exports that the internal
#                 backend of 3.46.0.6 needs on JDK 17 (RestCommunication uses sun.net.www.protocol handlers)
#   RUNS          sequential runs (default 1); TIMINGS_OUT file to store the aggregate in under the suite's key
#   WORKSPACE_DIR workspace folder for the notebooks (default: the current user's home)
#   SW_JAR / PY_PACKAGE  local jar / wheel to test instead of the released 3.46.0.6 artifacts
set -euo pipefail

source "$(dirname "$0")/common.sh"
: "${UPLOAD_PATH:?UPLOAD_PATH must be a Unity Catalog volume directory, e.g. /Volumes/<catalog>/<schema>/<volume>/tests}"
SPARK_VERSION="${SPARK_VERSION:-16.4.x-scala2.12}"
NODE_TYPE="${NODE_TYPE:-Standard_D4ds_v5}"
if [[ "$SUITE" == training ]]; then WORKERS="${WORKERS:-3}"; else WORKERS="${WORKERS:-1}"; fi
H2O_NODES="${H2O_NODES:-$WORKERS}"
if [[ "$SUITE" == training ]]; then
  JAVA_OPTIONS="${JAVA_OPTIONS---add-exports=java.base/sun.net.www.protocol.http=ALL-UNNAMED \
--add-exports=java.base/sun.net.www.protocol.https=ALL-UNNAMED}"
fi
RUNS="${RUNS:-1}"
dry_run=false
[[ "${1:-}" == "--dry-run" ]] && dry_run=true
work="$repo/build/databricks-runner"
mkdir -p "$work"

dbx() {
  if $dry_run; then echo "databricks$(printf " %q" "$@")" >&2; else databricks "$@"; fi
}

if $dry_run; then user="<current user>"; else user="$(databricks current-user me -o json | jq -r .userName)"; fi
WORKSPACE_DIR="${WORKSPACE_DIR:-/Workspace/Users/$user/sparkling-water-tests}"

# Uploads a local file to UPLOAD_PATH and prints its path there. `|| return` because errexit does not apply inside the
# command substitutions that call it; callers assign its output on its own so a failed upload stops the run.
upload() {
  dbx fs cp --overwrite "$1" "dbfs:$UPLOAD_PATH/$(basename "$1")" >/dev/null || return
  echo "$UPLOAD_PATH/$(basename "$1")"
}

dbx fs mkdirs "dbfs:$UPLOAD_PATH"
if [[ -n "${SW_JAR:-}" ]]; then
  jar_path="$(upload "$SW_JAR")"
  jar_lib="$(jq -n --arg p "$jar_path" '{jar: $p}')"
else
  jar_lib="$(jq -n --arg c "$sw_maven" '{maven: {coordinates: $c}}')"
fi
if [[ -f "$PY_PACKAGE" ]]; then
  py_path="$(upload "$PY_PACKAGE")"
  py_lib="$(jq -n --arg p "$py_path" '{whl: $p}')"
else
  py_lib="$(jq -n --arg p "$PY_PACKAGE" '{pypi: {package: $p}}')"
fi

# The Spark conf lines (valid Scala and Python) that pass the test inputs of the suite.
inputs() { # <task>
  if [[ "$SUITE" == scoring ]]; then
    echo "spark.conf.set(\"spark.sw.scoring.test.mojo\", \"$UPLOAD_PATH/$mojo_file\")"
    echo "spark.conf.set(\"spark.sw.scoring.test.data\", \"$UPLOAD_PATH/$data_file\")"
    echo "spark.conf.set(\"spark.sw.scoring.test.out\", \"$UPLOAD_PATH/out-$1\")"
  else
    echo "spark.conf.set(\"spark.sw.training.test.nodes\", \"$H2O_NODES\")"
  fi
}
if [[ "$SUITE" == scoring ]]; then
  upload "$fixtures/$mojo_file" >/dev/null
  upload "$fixtures/$data_file" >/dev/null
fi

# Wraps a test script into a notebook: input cell, timing helper and script, and a cell returning
# "<summary> | <marker> | <timings JSON>".
notebook() { # <task> <script extension> <comment prefix> <language> <timings JSON expression>
  local cell="$3 COMMAND ----------" source="$work/${SUITE}_$1"
  {
    echo "$3 Databricks notebook source"
    inputs "$1"
    printf '\n%s\n\n' "$cell"
    with_timing "$2" "$here/$SUITE/test.$2"
    printf '\n%s\n\n' "$cell"
    echo "dbutils.notebook.exit(summary + \" | $marker | \" + $5)"
  } >"$source"
  dbx workspace import "$WORKSPACE_DIR/${SUITE}_$1" --file "$source" --format SOURCE --language "$4" --overwrite
}
dbx workspace mkdirs "$WORKSPACE_DIR"
notebook scala scala "//" SCALA timingsJson
notebook python py "#" PYTHON "timings_json()"

jq -n --arg dir "$WORKSPACE_DIR/$SUITE" --arg cluster "${CLUSTER_ID:-}" --arg version "$SPARK_VERSION" \
  --arg node "$NODE_TYPE" --argjson workers "$WORKERS" --arg user "$user" --argjson jar "$jar_lib" \
  --argjson py "$py_lib" --arg java "${JAVA_OPTIONS:-}" '
  (if $java == "" then {} else {spark_conf: {"spark.driver.extraJavaOptions": $java,
    "spark.executor.extraJavaOptions": $java}} end) as $conf
  | (if $cluster != "" then {existing_cluster_id: $cluster} else {new_cluster: ({spark_version: $version,
    node_type_id: $node, num_workers: $workers, data_security_mode: "SINGLE_USER", single_user_name: $user} + $conf)}
    end) as $compute
  | def task($key; $lib): {task_key: $key, notebook_task: {notebook_path: "\($dir)_\($key)", source: "WORKSPACE"},
      libraries: [$lib]} + $compute;
  {timeout_seconds: 3600, tasks: [task("scala"; $jar), task("python"; $py)]}' >"$work/job-$SUITE.json"

run_name="sparkling-water $SUITE test"
if $dry_run; then
  dbx jobs submit --no-wait --run-name "$run_name" --json "@$work/job-$SUITE.json"
  cat "$work/job-$SUITE.json"
  exit 0
fi

# Prints the run state, retrying transient CLI failures; says which run is left behind if it gives up.
get_run() {
  for _ in 1 2 3 4 5; do
    databricks jobs get-run "$run_id" -o json && return
    sleep 10
  done
  echo "Cannot read the state of run $run_id; check or cancel it in the workspace." >&2
  return 1
}

# Submits one run, waits for it, checks both tasks and writes its timings to $work/timings-<suite>-<run id>.json.
run_once() {
  run_id="$(databricks jobs submit --no-wait --run-name "$run_name" --json "@$work/job-$SUITE.json" -o json |
    jq -r .run_id)"
  run="$(get_run)"
  echo "Run $run_id: $(jq -r .run_page_url <<<"$run")"
  until [[ "$(jq -r .state.life_cycle_state <<<"$run")" =~ ^(TERMINATED|SKIPPED|INTERNAL_ERROR)$ ]]; do
    sleep 30
    run="$(get_run)"
  done

  local failed=0 phases='{}'
  for task in scala python; do
    task_run="$(jq -r --arg t "$task" '.tasks[]? | select(.task_key == $t) | .run_id // empty' <<<"$run")"
    output='{}' # a task that never ran has no output; the run state below says why
    if [[ -n "$task_run" ]]; then output="$(databricks jobs get-run-output "$task_run" -o json)" || output='{}'; fi
    result="$(jq -r '.notebook_output.result // ""' <<<"$output")"
    if [[ "$result" == *" | $marker | "* ]] && jq -e . >/dev/null 2>&1 <<<"${result##* | }"; then
      echo "$task: PASSED (${result%% | *})"
      phases="$(jq --arg t "$task" --argjson p "${result##* | }" '.[$t] = $p' <<<"$phases")"
    else
      echo "$task: FAILED"
      jq -r '.metadata.state.state_message, .error, (.error_trace // "" | .[0:3000]) | select(. != null and . != "")' \
        <<<"$output"
      failed=1
    fi
  done
  if [[ "$failed" != 0 ]]; then
    jq -r '"Run: \(.state.result_state // .state.life_cycle_state) \(.state.state_message // "")"' <<<"$run"
    return 1
  fi
  # Platform phases in seconds; a task's library install is part of its setup.
  jq --arg suite "$SUITE" --arg sw "$SW_VERSION" --arg version "$SPARK_VERSION" --arg node "$NODE_TYPE" \
    --argjson workers "$WORKERS" --argjson phases "$phases" --arg java "${JAVA_OPTIONS:-}" '
    def s: (. // 0) / 1000;
    {suite: $suite, sw_version: $sw, spark_version: $version, node_type: $node, workers: $workers, java_options: $java,
     run_id: .run_id,
     tasks: ({run: {total: (.run_duration | s)}} + (reduce .tasks[] as $t ({}; .[$t.task_key] =
       {queue: ($t.queue_duration | s), setup: ($t.setup_duration | s), execution: ($t.execution_duration | s),
        cleanup: ($t.cleanup_duration | s)} + $phases[$t.task_key]))),
     libraries: [.tasks[] | {(.task_key): .libraries}] | add}' <<<"$run" >"$work/timings-$SUITE-$run_id.json"
}

records=()
for n in $(seq "$RUNS"); do
  echo "== $SUITE run $n of $RUNS"
  run_once
  records+=("$work/timings-$SUITE-$run_id.json")
done

# Aggregate: per task and phase, the median, min and max over the runs, in the phase order of the first run.
jq -s '
  def stats: sort | {median: (if length % 2 == 1 then .[length / 2 | floor] else (.[length / 2 - 1] + .[length / 2]) / 2 end),
    min: .[0], max: .[-1]};
  . as $runs | $runs[0] | {suite, sw_version, spark_version, node_type, workers, java_options, libraries,
    runs: ($runs | length),
    tasks: (.tasks | with_entries(.key as $t | .value |= with_entries(.key as $p | .value = ([$runs[].tasks[$t][$p]] | stats))))}' \
  "${records[@]}" >"$work/timings-$SUITE.json"
echo "Timings: $work/timings-$SUITE.json"
if [[ -n "${TIMINGS_OUT:-}" ]]; then
  [[ -f "$TIMINGS_OUT" ]] || echo '{}' >"$TIMINGS_OUT"
  jq --arg s "$SUITE" --slurpfile a "$work/timings-$SUITE.json" '.[$s] = $a[0]' "$TIMINGS_OUT" >"$TIMINGS_OUT.tmp"
  mv "$TIMINGS_OUT.tmp" "$TIMINGS_OUT"
  echo "Stored under \"$SUITE\" in $TIMINGS_OUT"
fi
