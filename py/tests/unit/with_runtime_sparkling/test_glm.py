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

import pyspark
from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pysparkling.ml.algos import H2OGLM
from pysparkling.ml.algos.classification import H2OGLMClassifier
from pysparkling.ml.algos.regression import H2OGLMRegressor
from tests import unit_test_utils
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGLM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGLM")


def testPipelineSerialization(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/glm_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/glm_pipeline_model"))

    loadedModel.transform(prostateDataset).count()


def testPropagationOfPredictionCol(prostateDataset):
    predictionCol = "my_prediction_col_name"
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8,
                  predictionCol=predictionCol)

    model = algo.fit(prostateDataset)
    columns = model.transform(prostateDataset).columns
    assert True == (predictionCol in columns)


def testInteractionPairsAffectResult(airlinesDataset):
    [traningDataset, testingDataset] = airlinesDataset.randomSplit([0.95, 0.05], 1)
    def createInitialGlmDefinition():
        return H2OGLM(
            seed=42,
            family="binomial",
            lambdaSearch=True,
            featuresCols=["Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier",
                         "CRSElapsedTime", "Origin", "Dest", "Distance"],
            labelCol="IsDepDelayed")

    referenceDeepLearning = createInitialGlmDefinition()
    referenceModel = referenceDeepLearning.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    glm = createInitialGlmDefinition()
    interactionPairs = [("CRSDepTime", "UniqueCarrier"),
                        ("CRSDepTime", "Origin"),
                        ("UniqueCarrier", "Origin")]
    glm.setInteractionPairs(interactionPairs)
    model = glm.fit(traningDataset)
    result = model.transform(testingDataset)

    # No check since interaction pairs are not supported yet. The purpose of this test is to check whether the fit
    # method does not throw an exception. Once interaction pairs are supported the assert should be uncommented.
    #
    # unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def testPlugValuesAffectResult(spark, carsDatasetPath):
    carsDataset = spark.read.csv(carsDatasetPath, header=True, inferSchema=True)
    carsDataset = carsDataset.withColumn("economy_20mpg", carsDataset.economy_20mpg.cast("string"))
    [traningDataset, testingDataset] = carsDataset.randomSplit([0.9, 0.1], 1)

    def createInitialGlmDefinition():
        featuresCols = ["economy", "displacement", "power", "weight", "acceleration", "year", "economy_20mpg"]
        return H2OGLM(featuresCols=featuresCols, labelCol="cylinders", seed=1, splitRatio=0.8)

    referenceGlm = createInitialGlmDefinition()
    referenceModel = referenceGlm.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    plugValues = {
        "economy": 1.1,
        "displacement": 2.2,
        "power": 3.3,
        "weight": 4.4,
        "acceleration": 5.5,
        "year": 2000,
        "economy_20mpg": "0"}
    glm = createInitialGlmDefinition()
    glm.setMissingValuesHandling("PlugValues")
    glm.setPlugValues(plugValues)
    model = glm.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def testInteractionColumnNamesArePassedWithoutException(spark):
    data = [(0.0, "a", 2.0),
            (float("nan"), "b", 8.0),
            (0.0, "a", 4.0),
            (1.0, "b", 1.0)]
    df = spark.createDataFrame(data, ["x", "y", "z"])

    plugValues = {"x": 0, "x_y.a": 1, "x_y.b": 2, "y": "b"}
    glm = H2OGLM(
        labelCol="z",
        seed=42,
        ignoreConstCols=False,
        standardize=False,
        family="gaussian",
        missingValuesHandling="PlugValues",
        plugValues=plugValues)

    glm.fit(df)


def createInitialGlmDefinitionForRandomCols():
    return H2OGLM(featuresCols=["x1", "x3", "x5", "x6"],
                  labelCol="y",
                  family="gaussian",
                  randomFamily=["gaussian"],
                  randomLink=["identity"],
                  HGLM=True,
                  calcLike=True)


@pytest.mark.skip(reason="HGLM doesn't support MOJOs yet.")
def testRandomColsArePropagatedToInternals(semiconductorDataset):
    semiconductorDataset = semiconductorDataset.withColumn("Device", semiconductorDataset.Device.cast("string"))

    referenceDeepLearning = createInitialGlmDefinitionForRandomCols()
    with pytest.raises(Py4JJavaError, match=r".*Need to specify the random component columns for HGLM.*"):
        referenceDeepLearning.fit(semiconductorDataset)

    glm = createInitialGlmDefinitionForRandomCols()
    glm.setRandomCols(["Device"])
    glm.fit(semiconductorDataset)


def testRandomColsMustBeWithinTrainingDataset(semiconductorDataset):
    glm = createInitialGlmDefinitionForRandomCols()
    glm.setRandomCols(["someColumn"])
    if pyspark.__version__ >= "3.4":
        pattern = r".*A column or function parameter with name `someColumn` cannot be resolved.*"
    elif pyspark.__version__ >= "3.3":
        pattern = r".*Column '.?someColumn.?' does not exist.*"
    else:
        pattern = r".*cannot resolve '.?someColumn.?' given input columns.*"

    with pytest.raises(AnalysisException, match=pattern):
        glm.fit(semiconductorDataset)


def testBetaConstraintsAffectResult(spark, prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)
    featuresCols = ["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]

    def createInitialGlmDefinition():
        return H2OGLM(featuresCols=featuresCols, labelCol="CAPSULE", seed=1, splitRatio=0.8)

    referenceGlm = createInitialGlmDefinition()
    referenceModel = referenceGlm.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    betaConstraints = map(lambda feature: (feature, -1000, 1000, 1, 0.2), featuresCols)
    betaConstraintsFrame = spark.createDataFrame(
        betaConstraints,
        ['names', 'lower_bounds', 'upper_bounds', 'beta_given', 'rho'])

    glm = createInitialGlmDefinition()
    glm.setBetaConstraints(betaConstraintsFrame)
    model = glm.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def setParamtersForProblemSpecificTests(glm):
    glm.setLabelCol("CAPSULE")
    glm.setSeed(1),
    glm.setSplitRatio(0.8)
    return glm


def testH2OGLMClassifierBehavesTheSameAsGenericH2OGLMOnStringLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    glm = setParamtersForProblemSpecificTests(H2OGLM())
    referenceModel = glm.fit(trainingDateset.withColumn("CAPSULE", col("CAPSULE").cast("string")))
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParamtersForProblemSpecificTests(H2OGLMClassifier())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OGLMRegressorBehavesTheSameAsGenericH2OGLMOnNumericLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    automl = setParamtersForProblemSpecificTests(H2OGLM())
    referenceModel = automl.fit(trainingDateset)
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParamtersForProblemSpecificTests(H2OGLMRegressor())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OGLMClassifierBehavesDiffenrentlyThanH2OGLMRegressor(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    regressor = setParamtersForProblemSpecificTests(H2OGLMRegressor())
    regressionModel = regressor.fit(trainingDateset)
    regressionDataset = regressionModel.transform(testingDataset).drop("detailed_prediction")

    classifier = setParamtersForProblemSpecificTests(H2OGLMClassifier())
    classificationModel = classifier.fit(trainingDateset)
    classificationDataset = classificationModel.transform(testingDataset).drop("detailed_prediction")

    unit_test_utils.assert_data_frames_have_different_values(regressionDataset, classificationDataset)
