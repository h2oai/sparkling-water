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
from tests import unit_test_utils

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2ODRF")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2ODRF")


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


def testCalibrationDataFrameCauseGenerationOfCalibratedProbabilities(prostateDataset):
    prostateDataset = prostateDataset.withColumn("CAPSULE", prostateDataset.CAPSULE.cast("string"))
    [trainingDataset, testingDataset, calibrationDataset] = prostateDataset.randomSplit([0.9, 0.05, 0.05], 1)

    algo = H2ODRF(featuresCols=["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="CAPSULE",
                  seed=1,
                  splitRatio=0.8,
                  calibrateModel=True,
                  calibrationDataFrame=calibrationDataset,
                  withDetailedPredictionCol=True)
    model = algo.fit(trainingDataset)
    result = model.transform(testingDataset).cache()
    probabilities = result.select("ID", "detailed_prediction.probabilities.0", "detailed_prediction.probabilities.1")
    calibrated = result.select(
        "ID",
        "detailed_prediction.calibratedProbabilities.0",
        "detailed_prediction.calibratedProbabilities.1")

    unit_test_utils.assert_data_frames_have_different_values(probabilities, calibrated)
