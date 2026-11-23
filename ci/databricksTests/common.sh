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
# Shared defaults for the test runners; sourced by run_local.sh and run_databricks.sh.
# SUITE selects the test scripts and the released artifacts they run against: scoring (default), training, or
# autoscale (the autoscaling probe, which uses the training artifacts).
# shellcheck disable=SC2034

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
SW_VERSION="${SW_VERSION:-3.46.0.6-1-3.5}"
SCALA_BINARY="${SCALA_BINARY:-2.12}"
SUITE="${SUITE:-scoring}"
case "$SUITE" in
  scoring) sw_artifact="sparkling-water-scoring-package_$SCALA_BINARY" py_default="h2o_pysparkling_scoring_3.5" ;;
  training | autoscale) sw_artifact="sparkling-water-package_$SCALA_BINARY" py_default="h2o_pysparkling_3.5" ;;
  *) echo "Unknown SUITE: $SUITE (scoring, training or autoscale)" >&2 && exit 1 ;;
esac
PY_PACKAGE="${PY_PACKAGE:-$py_default==3.46.0.6.post1}"
sw_maven="ai.h2o:$sw_artifact:$SW_VERSION"
fixtures="$repo/ml/src/test/resources"
mojo_file="binom_model_prostate.mojo"
data_file="binom_model_prostate_prediction.csv"
marker="TEST PASSED"

# Writes the timing helper for a language (scala or py) followed by a test script to stdout.
with_timing() { # <extension> <script>
  cat "$here/timing.$1" && echo && cat "$2"
}
