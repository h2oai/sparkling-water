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
from pyspark.sql.functions import *
from pysparkling.ml import H2OIsolationForest
from tests import unit_test_utils

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OIsolationForest")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OIsolationForest")


def testPipelineSerialization(prostateDataset):
    algo = H2OIsolationForest(seed=1)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/isolation_forest_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/isolation_forest_pipeline"))
    model = loadedPipeline.fit(prostateDataset)
    expected = model.transform(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/isolation_forest_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/isolation_forest_pipeline_model"))
    result = loadedModel.transform(prostateDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testIsolationForestModelGiveDifferentPredictionsOnDifferentRecords(prostateDataset):
    [trainingDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)
    algo = H2OIsolationForest(seed=1)
    model = algo.fit(trainingDataset)

    result = model.transform(testingDataset)
    predictions = result.select("prediction").take(2)

    assert(predictions[0][0] != predictions[1][0])


def testExplicitValidationFrameOnIsolationForest(spark, prostateDataset):
    validationDatasetPath = "file://" + os.path.abspath("../examples/smalldata/prostate/prostate_anomaly_validation.csv")
    validatationDataset = spark.read.csv(validationDatasetPath, header=True, inferSchema=True)

    algo = H2OIsolationForest(seed=1, validationDataFrame=validatationDataset, validationLabelCol="isAnomaly")
    model = algo.fit(prostateDataset)
    metrics = model.getValidationMetrics()

    assert(metrics['AUC'] > 0.9)
    assert(metrics['Logloss'] < 1.0)
