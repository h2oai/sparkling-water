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
# Runs the scoring-only test scripts (test.scala, test.py) against a local Spark distribution.
# Defaults test the released 3.46.0.6 scoring artifacts; override them to test a local build:
#   SCORING_JAR=<path to scoring jar>  PY_PACKAGE=<path to scoring wheel or sdist>
set -euo pipefail

source "$(dirname "$0")/common.sh"
: "${SPARK_HOME:?SPARK_HOME must point to a Spark 3.5.x distribution}"
PYTHON="${PYTHON:-python3.11}"
work="$repo/build/scoring-runner"
mkdir -p "$work"

if [[ -n "${SCORING_JAR:-}" ]]; then # never replace a requested local build with the released jar
  [[ -f "$SCORING_JAR" ]] || { echo "SCORING_JAR not found: $SCORING_JAR" >&2; exit 1; }
  jar="$SCORING_JAR"
else
  jar="$work/$scoring_artifact-$SW_VERSION.jar"
  if [[ ! -f "$jar" ]]; then
    curl -fsSL -o "$jar" "https://repo1.maven.org/maven2/ai/h2o/$scoring_artifact/$SW_VERSION/$scoring_artifact-$SW_VERSION.jar"
  fi
fi

if [[ ! -x "$work/venv/bin/python" ]]; then
  "$PYTHON" -m venv "$work/venv"
fi
# PySparkling <= 3.46.0.6 imports pkg_resources, which recent setuptools releases removed.
"$work/venv/bin/pip" install -q --upgrade "numpy<2" "setuptools<80" "$PY_PACKAGE"
if [[ -f "$PY_PACKAGE" ]]; then # pip skips a local wheel whose version is installed; reinstall a rebuilt one
  "$work/venv/bin/pip" install -q --force-reinstall --no-deps "$PY_PACKAGE"
fi

# Loopback driver address: on some hosts the default address breaks Spark's local jar transfer ("Too large frame").
# Sets conf_args to the Spark conf for one task; an array keeps paths with spaces intact.
set_conf_args() {
  conf_args=(--conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1
    --conf "spark.sw.scoring.test.mojo=file://$fixtures/$mojo_file"
    --conf "spark.sw.scoring.test.data=file://$fixtures/$data_file"
    --conf "spark.sw.scoring.test.out=file://$work/out-$1")
}

status=0
check() {
  if grep -q "$marker" "$work/$1.log"; then
    echo "$1: PASSED"
  else
    echo "$1: FAILED (log: $work/$1.log)"
    tail -n 40 "$work/$1.log"
    status=1
  fi
}

# spark-shell -i silently ignores a script path containing spaces, so load a temporary copy.
scala_script="$(mktemp "${TMPDIR:-/tmp}/sw-scoring-test.XXXXXX")"
trap 'rm -f "$scala_script"' EXIT
cp "$here/test.scala" "$scala_script"
set_conf_args scala
"$SPARK_HOME/bin/spark-shell" --master "local[2]" --jars "$jar" "${conf_args[@]}" -i "$scala_script" \
  </dev/null >"$work/scala.log" 2>&1 || true
check scala

set_conf_args python
PYSPARK_PYTHON="$work/venv/bin/python" "$SPARK_HOME/bin/spark-submit" --master "local[2]" "${conf_args[@]}" \
  "$here/test.py" >"$work/python.log" 2>&1 || true
check python

exit "$status"
