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

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../../.." && pwd)"
: "${SPARK_HOME:?SPARK_HOME must point to a Spark 3.5.x distribution}"
SW_VERSION="${SW_VERSION:-3.46.0.6-1-3.5}"
SCALA_BINARY="${SCALA_BINARY:-2.12}"
PY_PACKAGE="${PY_PACKAGE:-h2o_pysparkling_scoring_3.5==3.46.0.6.post1}"
PYTHON="${PYTHON:-python3.11}"
work="$repo/build/scoring-runner"
mkdir -p "$work"

jar="${SCORING_JAR:-$work/sparkling-water-scoring-package_$SCALA_BINARY-$SW_VERSION.jar}"
if [[ ! -f "$jar" ]]; then
  artifact="sparkling-water-scoring-package_$SCALA_BINARY"
  curl -fsSL -o "$jar" "https://repo1.maven.org/maven2/ai/h2o/$artifact/$SW_VERSION/$artifact-$SW_VERSION.jar"
fi

if [[ ! -x "$work/venv/bin/python" ]]; then
  "$PYTHON" -m venv "$work/venv"
fi
# PySparkling <= 3.46.0.6 imports pkg_resources, which recent setuptools releases removed.
"$work/venv/bin/pip" install -q --upgrade "numpy<2" "setuptools<80" "$PY_PACKAGE"

fixtures="file://$repo/ml/src/test/resources"
# Loopback driver address: on some hosts the default address breaks Spark's local jar transfer ("Too large frame").
conf() {
  echo --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
    --conf "spark.sw.scoring.test.mojo=$fixtures/binom_model_prostate.mojo" \
    --conf "spark.sw.scoring.test.data=$fixtures/binom_model_prostate_prediction.csv" \
    --conf "spark.sw.scoring.test.out=file://$work/out-$1"
}

status=0
check() {
  if grep -q "SCORING TEST PASSED" "$work/$1.log"; then
    echo "$1: PASSED"
  else
    echo "$1: FAILED (log: $work/$1.log)"
    tail -n 40 "$work/$1.log"
    status=1
  fi
}

# shellcheck disable=SC2046
"$SPARK_HOME/bin/spark-shell" --master "local[2]" --jars "$jar" $(conf scala) -i "$here/test.scala" \
  </dev/null >"$work/scala.log" 2>&1 || true
check scala

# shellcheck disable=SC2046
PYSPARK_PYTHON="$work/venv/bin/python" "$SPARK_HOME/bin/spark-submit" --master "local[2]" $(conf python) \
  "$here/test.py" >"$work/python.log" 2>&1 || true
check python

exit "$status"
