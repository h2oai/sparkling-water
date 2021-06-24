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
import os

from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import col, regexp_extract, length

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


def prepareLeaderboardForComparison(df):
    return df.withColumn("model_id", regexp_extract(col("model_id"), "(.*)_AutoML_[\d_]+((?:_.*)?)$", 1)).drop("")


def testLeaderboardDataFrameHasImpactOnAutoMLLeaderboard(classificationDataset):
    [trainingDateset, testingDataset] = classificationDataset.randomSplit([0.9, 0.1], 42)

    automl = setParametersForTesting(H2OAutoML())
    automl.fit(trainingDateset)
    defaultLeaderboard1 = prepareLeaderboardForComparison(automl.getLeaderboard())

    automl = setParametersForTesting(H2OAutoML())
    automl.fit(trainingDateset)
    defaultLeaderboard2 = prepareLeaderboardForComparison(automl.getLeaderboard())

    automl = setParametersForTesting(H2OAutoML()).setLeaderboardDataFrame(testingDataset)
    automl.fit(trainingDateset)
    leaderboardWithCustomDataFrameSet = prepareLeaderboardForComparison(automl.getLeaderboard())

    defaultLeaderboard1.show(truncate=False)
    defaultLeaderboard2.show(truncate=False)
    unit_test_utils.assert_data_frames_are_identical(defaultLeaderboard1, defaultLeaderboard2)
    unit_test_utils.assert_data_frames_have_different_values(defaultLeaderboard1, leaderboardWithCustomDataFrameSet)


def testDeserializationOfUnfittedPipelineWithAutoML(classificationDataset):
    [trainingDateset, testingDataset] = classificationDataset.randomSplit([0.9, 0.1], 42)

    algo = setParametersForTesting(H2OAutoML()).setLeaderboardDataFrame(testingDataset)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/automl_pipeline_leaderboardDF"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/automl_pipeline_leaderboardDF"))
    loadedPipeline.fit(trainingDateset)


def testBlendingDataFrameHasImpactOnAutoMLStackedEnsambleModels(classificationDataset):
    [trainingDateset, blendingDataset] = classificationDataset.randomSplit([0.9, 0.1], 42)

    def separateEnsembleModels(df):
        stackedEnsembleDF = df.filter(df.model_id.startswith('StackedEnsemble'))
        othersDF = df.subtract(stackedEnsembleDF)
        return (stackedEnsembleDF, othersDF)

    automl = setParametersForTesting(H2OAutoML())
    automl.fit(trainingDateset)
    defaultLeaderboard = separateEnsembleModels(prepareLeaderboardForComparison(automl.getLeaderboard()))

    automl = setParametersForTesting(H2OAutoML()).setBlendingDataFrame(blendingDataset)
    automl.fit(trainingDateset)
    leaderboardWithBlendingFrameSet = separateEnsembleModels(prepareLeaderboardForComparison(automl.getLeaderboard()))

    assert defaultLeaderboard[0].count() == 2
    unit_test_utils.assert_data_frames_have_different_values(defaultLeaderboard[0], leaderboardWithBlendingFrameSet[0])
    unit_test_utils.assert_data_frames_are_identical(defaultLeaderboard[1], leaderboardWithBlendingFrameSet[1])


def testDeserializationOfUnfittedPipelineWithAutoML(classificationDataset):
    [trainingDateset, blendingDataset] = classificationDataset.randomSplit([0.9, 0.1], 42)

    algo = setParametersForTesting(H2OAutoML()).setBlendingDataFrame(blendingDataset)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/automl_pipeline_blendingDF"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/automl_pipeline_blendingDF"))
    loadedPipeline.fit(trainingDateset)


def testScoringWithAllLeaderbordModels(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)
    automl = setParametersForTesting(H2OAutoML())
    automl.fit(trainingDateset)
    for model in automl.getAllModels():
        model.transform(testingDataset).collect()
