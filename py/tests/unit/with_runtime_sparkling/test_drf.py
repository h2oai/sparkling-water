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

import os
from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pysparkling.ml import H2ODRF

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    # Skipping testing of algo option as we don't generate equal algo
    assertParamsViaConstructor("H2ODRF", ["algo"])


def testParamsPassedBySetters():
    # Skipping testing of algo option as we don't generate equal algo
    assertParamsViaSetters("H2ODRF", ["algo"])


def testPipelineSerialization(prostateDataset):
    algo = H2ODRF(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/drf_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/drf_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/drf_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/drf_pipeline_model"))

    loadedModel.transform(prostateDataset).count()
