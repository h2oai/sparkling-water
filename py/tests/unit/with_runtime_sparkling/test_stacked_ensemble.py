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

import pytest
import os
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col
from pysparkling.ml.algos import H2OStackedEnsemble, H2OGLM, H2OGBM
from tests import unit_test_utils

from tests.unit.with_runtime_sparkling.algo_test_utils import *

@pytest.fixture(scope="module")
def classificationDataset(prostateDataset):
    return prostateDataset.withColumn("CAPSULE", col("CAPSULE").cast("string"))

def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OStackedEnsemble")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OStackedEnsemble")


def setParametersForTesting(algo, foldsNo):
    algo.setLabelCol("AGE")
    if (foldsNo > 0) :
        algo.setNfolds(foldsNo)
        algo.setFoldAssignment("Modulo")
        algo.setKeepCrossValidationPredictions(True)
    algo.setKeepBinaryModels(True)
    algo.setSeed(42)
    return algo

def testStackedEnsembleUsingCrossValidations(classificationDataset):
    foldsNo = 5
    glm = setParametersForTesting(H2OGLM(), foldsNo)
    glm_model = glm.fit(classificationDataset)

    gbm = setParametersForTesting(H2OGBM(), foldsNo)
    gbm_model = gbm.fit(classificationDataset)

    ensemble = H2OStackedEnsemble()
    ensemble.setBaseModels([glm_model, gbm_model])
    ensemble.setLabelCol("AGE")

    ensemble.fit(classificationDataset)


def testStackedEnsembleUsingBlendingFrame(classificationDataset):
    [trainingDateset, blendingDataset] = classificationDataset.randomSplit([0.6, 0.4], 42)
    glm = setParametersForTesting(H2OGLM(), foldsNo = 0)
    glm_model = glm.fit(trainingDateset)

    gbm = setParametersForTesting(H2OGBM(), foldsNo = 0)
    gbm_model = gbm.fit(trainingDateset)

    ensemble = H2OStackedEnsemble()
    ensemble.setBlendingDataFrame(blendingDataset)
    ensemble.setBaseModels([glm_model, gbm_model])
    ensemble.setLabelCol("AGE")

    ensemble.fit(trainingDateset)


# def testStackedEnsembleKeepsBaseModelsWhenRequested(classificationDataset):
#     foldsNo = 5
#     glm = setParametersForTesting(H2OGLM(), foldsNo)
#     glm_model = glm.fit(classificationDataset)
#
#     gbm = setParametersForTesting(H2OGBM(), foldsNo)
#     gbm_model = gbm.fit(classificationDataset)
#
#     ensemble = H2OStackedEnsemble()
#     ensemble.setBaseModels([glm_model, gbm_model])
#     ensemble.setDeleteBaseModels(False)
#     ensemble.setLabelCol("AGE")
#
#     ensemble.fit(classificationDataset)

