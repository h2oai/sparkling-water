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

"""
Unit tests for PySparkling H2OAutoML
"""
import pytest

from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import col

from pysparkling.ml.algos import H2OAutoML
from pysparkling.ml.algos.classification import H2OAutoMLClassifier
from pysparkling.ml.algos.regression import H2OAutoMLRegressor

from tests import unit_test_utils
from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OAutoML")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OAutoML")


@pytest.fixture(scope="module")
def classificationDataset(prostateDataset):
    return prostateDataset.withColumn("CAPSULE", col("CAPSULE").cast("string"))


def setParametersForTesting(automl):
    automl.setLabelCol("CAPSULE")
    automl.setIgnoredCols(["ID"])
    automl.setExcludeAlgos(["GLM"])
    automl.setMaxModels(5)
    automl.setSeed(42)
    return automl


def testGetLeaderboardWithListAsArgument(classificationDataset):
    automl = setParametersForTesting(H2OAutoML())
    automl.fit(classificationDataset)
    extraColumns = ["training_time_ms", "predict_time_per_row_ms"]
    assert automl.getLeaderboard(extraColumns).columns == automl.getLeaderboard().columns + extraColumns


def testGetLeaderboardWithVariableArgumens(classificationDataset):
    automl = setParametersForTesting(H2OAutoML())
    automl.fit(classificationDataset)
    extraColumns = ["training_time_ms", "predict_time_per_row_ms"]
    result = automl.getLeaderboard("training_time_ms", "predict_time_per_row_ms").columns
    expected = automl.getLeaderboard().columns + extraColumns
    assert result == expected


def testH2OAutoMLClassifierBehavesTheSameAsGenericH2OAutoMLOnStringLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    automl = setParametersForTesting(H2OAutoML())
    referenceModel = automl.fit(trainingDateset.withColumn("CAPSULE", col("CAPSULE").cast("string")))
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParametersForTesting(H2OAutoMLClassifier())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OAutoMLRegressorBehavesTheSameAsGenericH2OAutoMLOnNumericLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    automl = setParametersForTesting(H2OAutoML())
    referenceModel = automl.fit(trainingDateset)
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParametersForTesting(H2OAutoMLRegressor())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OAutoMLClassifierBehavesDiffenrentlyThanH2OAutoMLRegressor(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    regressor = setParametersForTesting(H2OAutoMLRegressor())
    regressionModel = regressor.fit(trainingDateset)
    regressionDataset = regressionModel.transform(testingDataset).drop("detailed_prediction")

    classifier = setParametersForTesting(H2OAutoMLClassifier())
    classificationModel = classifier.fit(trainingDateset)
    classificationDataset = classificationModel.transform(testingDataset).drop("detailed_prediction")

    unit_test_utils.assert_data_frames_have_different_values(regressionDataset, classificationDataset)
