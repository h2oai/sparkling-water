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
# Runs the scoring-only test scripts (test.scala, test.py) as notebook tasks of one Databricks job run.
# Usage: run_databricks.sh [--dry-run]   (--dry-run prints the CLI calls and the job spec, contacts nothing)
# Uses the logged-in databricks CLI (select a workspace with DATABRICKS_CONFIG_PROFILE). Parameters, never committed:
#   UPLOAD_PATH   Unity Catalog volume directory for fixtures and outputs, e.g. /Volumes/<catalog>/<schema>/<volume>/scoring
#   CLUSTER_ID    existing dedicated-access cluster; unset = a new job cluster per task (SPARK_VERSION, NODE_TYPE)
#   WORKSPACE_DIR workspace folder for the notebooks (default: the current user's home)
#   SCORING_JAR / PY_PACKAGE  local scoring jar / wheel to test instead of the released 3.46.0.6 artifacts
set -euo pipefail

source "$(dirname "$0")/common.sh"
: "${UPLOAD_PATH:?UPLOAD_PATH must be a Unity Catalog volume directory, e.g. /Volumes/<catalog>/<schema>/<volume>/scoring}"
SPARK_VERSION="${SPARK_VERSION:-16.4.x-scala2.12}"
NODE_TYPE="${NODE_TYPE:-Standard_D4ds_v5}"
dry_run=false
[[ "${1:-}" == "--dry-run" ]] && dry_run=true
work="$repo/build/scoring-runner-databricks"
mkdir -p "$work"

dbx() {
  if $dry_run; then echo "databricks$(printf " %q" "$@")" >&2; else databricks "$@"; fi
}

if $dry_run; then user="<current user>"; else user="$(databricks current-user me -o json | jq -r .userName)"; fi
WORKSPACE_DIR="${WORKSPACE_DIR:-/Workspace/Users/$user/sparkling-water-scoring-test}"

# Uploads a local file to UPLOAD_PATH and prints its path there.
upload() {
  dbx fs cp --overwrite "$1" "dbfs:$UPLOAD_PATH/$(basename "$1")" >/dev/null
  echo "$UPLOAD_PATH/$(basename "$1")"
}

dbx fs mkdirs "dbfs:$UPLOAD_PATH"
upload "$fixtures/$mojo_file" >/dev/null
upload "$fixtures/$data_file" >/dev/null
if [[ -n "${SCORING_JAR:-}" ]]; then
  jar_lib="$(jq -n --arg p "$(upload "$SCORING_JAR")" '{jar: $p}')"
else
  jar_lib="$(jq -n --arg c "$scoring_maven" '{maven: {coordinates: $c}}')"
fi
if [[ -f "$PY_PACKAGE" ]]; then
  py_lib="$(jq -n --arg p "$(upload "$PY_PACKAGE")" '{whl: $p}')"
else
  py_lib="$(jq -n --arg p "$PY_PACKAGE" '{pypi: {package: $p}}')"
fi

# Wraps a test script into a notebook: a cell setting the test inputs, the script, and a cell returning its summary.
notebook() { # <task> <script> <comment prefix> <language>
  local cell="$3 COMMAND ----------" source="$work/test_$1"
  {
    echo "$3 Databricks notebook source"
    for key in mojo data; do
      file="$mojo_file"; [[ $key == data ]] && file="$data_file"
      echo "spark.conf.set(\"spark.sw.scoring.test.$key\", \"$UPLOAD_PATH/$file\")"
    done
    echo "spark.conf.set(\"spark.sw.scoring.test.out\", \"$UPLOAD_PATH/out-$1\")"
    printf '\n%s\n\n' "$cell"
    cat "$2"
    printf '\n%s\n\n' "$cell"
    echo "dbutils.notebook.exit(summary + \" | $marker\")"
  } >"$source"
  dbx workspace import "$WORKSPACE_DIR/test_$1" --file "$source" --format SOURCE --language "$4" --overwrite
}
dbx workspace mkdirs "$WORKSPACE_DIR"
notebook scala "$here/test.scala" "//" SCALA
notebook python "$here/test.py" "#" PYTHON

jq -n --arg dir "$WORKSPACE_DIR" --arg cluster "${CLUSTER_ID:-}" --arg version "$SPARK_VERSION" \
  --arg node "$NODE_TYPE" --arg user "$user" --argjson jar "$jar_lib" --argjson py "$py_lib" '
  (if $cluster != "" then {existing_cluster_id: $cluster} else {new_cluster: {spark_version: $version,
    node_type_id: $node, num_workers: 1, data_security_mode: "SINGLE_USER", single_user_name: $user}} end) as $compute
  | def task($key; $lib): {task_key: $key, notebook_task: {notebook_path: "\($dir)/test_\($key)", source: "WORKSPACE"},
      libraries: [$lib]} + $compute;
  {timeout_seconds: 3600, tasks: [task("scala"; $jar), task("python"; $py)]}' >"$work/job.json"

if $dry_run; then
  dbx jobs submit --no-wait --run-name "sparkling-water scoring test" --json "@$work/job.json"
  cat "$work/job.json"
  exit 0
fi
run_id="$(databricks jobs submit --no-wait --run-name "sparkling-water scoring test" --json "@$work/job.json" -o json |
  jq -r .run_id)"
run="$(databricks jobs get-run "$run_id" -o json)"
echo "Run $run_id: $(jq -r .run_page_url <<<"$run")"
until [[ "$(jq -r .state.life_cycle_state <<<"$run")" =~ ^(TERMINATED|SKIPPED|INTERNAL_ERROR)$ ]]; do
  sleep 30
  run="$(databricks jobs get-run "$run_id" -o json)"
done

status=0
for task in scala python; do
  output="$(databricks jobs get-run-output "$(jq -r --arg t "$task" '.tasks[] | select(.task_key == $t) | .run_id' \
    <<<"$run")" -o json)"
  result="$(jq -r '.notebook_output.result // ""' <<<"$output")"
  if [[ "$result" == *"$marker"* ]]; then
    echo "$task: PASSED ($result)"
  else
    echo "$task: FAILED"
    jq -r '.metadata.state.state_message // "", .error // "", (.error_trace // "" | .[0:3000])' <<<"$output"
    status=1
  fi
done
exit "$status"
