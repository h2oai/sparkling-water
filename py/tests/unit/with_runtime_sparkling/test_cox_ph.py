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
import pytest
import json

from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pysparkling.ml import H2OCoxPH

from tests import unit_test_utils
from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OCoxPH")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OCoxPH")


def testPipelineSerialization(heartDataset):
    features = ['age', 'year', 'surgery', 'transplant', 'start', 'stop']
    algo = H2OCoxPH(labelCol="event", featuresCols=features, startCol='start', stopCol='stop')

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/cox_ph_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/cox_ph_pipeline"))
    model = loadedPipeline.fit(heartDataset)
    expected = model.transform(heartDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/cox_ph_pipeline"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/cox_ph_pipeline"))
    result = loadedModel.transform(heartDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testCoxPHModelGivesDifferentPredictionsOnDifferentRecords(heartDataset):
    [trainingDataset, testingDataset] = heartDataset.randomSplit([0.9, 0.1], 42)
    features = ['age', 'year', 'surgery', 'transplant', 'start', 'stop']
    algo = H2OCoxPH(labelCol="event", featuresCols=features, startCol='start', stopCol='stop')

    model = algo.fit(trainingDataset)

    result = model.transform(testingDataset)
    predictions = result.select("prediction").take(2)

    assert(predictions[0][0] != predictions[1][0])

def testCoxPHModelGivesDifferentPredictionsWithDifferentParameters(heartDataset):
    def fit_and_predict(testingDataset, trainingDataset, ties):
        features = ['age', 'year', 'surgery', 'transplant', 'start', 'stop']
        algo = H2OCoxPH(labelCol="event", featuresCols=features, startCol='start', stopCol='stop', ties=ties)
        model = algo.fit(trainingDataset)
        result = model.transform(testingDataset)
        return result

    [trainingDataset, testingDataset] = heartDataset.randomSplit([0.9, 0.1], 42)

    result1 = fit_and_predict(testingDataset, trainingDataset, "efron")
    predictions1 = result1.select("prediction").take(1)

    result2 = fit_and_predict(testingDataset, trainingDataset, "breslow")
    predictions2 = result2.select("prediction").take(1)

    assert(predictions1[0][0] != predictions2[0][0])


