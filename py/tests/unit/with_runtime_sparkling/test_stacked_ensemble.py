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
from pyspark.ml import Pipeline
from pyspark.sql.functions import col
from pysparkling.ml.algos import H2OStackedEnsemble, H2OGLM, H2OGBM, H2ODRF


from tests.unit.with_runtime_sparkling.algo_test_utils import *

@pytest.fixture(scope="module")
def classificationDataset(prostateDataset):
    return prostateDataset.withColumn("CAPSULE", col("CAPSULE").cast("string"))

@pytest.fixture(scope="module")
def trainingDataset(classificationDataset):
    return classificationDataset.where("ID <= 300").cache()

@pytest.fixture(scope="module")
def testingDataset(classificationDataset):
    return classificationDataset.where("ID > 300").cache()


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OStackedEnsemble")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OStackedEnsemble")


def setParametersForTesting(algo, foldsNo):
    algo.setLabelCol("CAPSULE")
    if (foldsNo > 0) :
        algo.setNfolds(foldsNo)
        algo.setFoldAssignment("Modulo")
        algo.setKeepCrossValidationPredictions(True)
    algo.setKeepBinaryModels(True)
    algo.setSeed(42)
    return algo


def testStackedEnsembleUsingCrossValidations(trainingDataset, testingDataset):
    foldsNo = 5
    glm = setParametersForTesting(H2OGLM(), foldsNo)
    gbm = setParametersForTesting(H2OGBM(), foldsNo)

    ensemble = H2OStackedEnsemble()
    ensemble.setBaseAlgorithms([glm, gbm])
    ensemble.setLabelCol("CAPSULE")
    ensemble.setSeed(42)

    model = ensemble.fit(trainingDataset)
    predictions = model.transform(testingDataset).select("detailed_prediction")
    assert predictions.distinct().count() == 80


def testStackedEnsembleUsingBlendingFrame(trainingDataset, testingDataset):

    training = trainingDataset.where("ID <= 200")
    blending = trainingDataset.where("ID > 200")

    drf = setParametersForTesting(H2ODRF(), foldsNo = 0)
    gbm = setParametersForTesting(H2OGBM(), foldsNo = 0)

    ensemble = H2OStackedEnsemble()
    ensemble.setBlendingDataFrame(blending)
    ensemble.setBaseAlgorithms([drf, gbm])
    ensemble.setLabelCol("CAPSULE")
    ensemble.setSeed(42)

    model = ensemble.fit(training)

    predictions = model.transform(testingDataset).select("detailed_prediction")
    assert predictions.distinct().count() == 57


def testDeserializationOfUnfittedPipelineWithStackedEnsemble(trainingDataset, testingDataset):
    foldsNo = 5
    glm = setParametersForTesting(H2OGLM(), foldsNo)
    gbm = setParametersForTesting(H2OGBM(), foldsNo)

    ensemble = H2OStackedEnsemble()
    ensemble.setBaseAlgorithms([glm, gbm])
    ensemble.setLabelCol("CAPSULE")
    ensemble.setSeed(42)

    pipeline = Pipeline(stages=[ensemble])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/stacked_ensemble_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/stacked_ensemble_pipeline"))

    model = loadedPipeline.fit(trainingDataset)
    predictions = model.transform(testingDataset).select("detailed_prediction")
    assert predictions.distinct().count() == 80

