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
from pyspark.sql.types import *
from pyspark.sql.functions import col
from pysparkling.ml.algos import H2OGAM
from pysparkling.ml.algos.classification import H2OGAMClassifier
from pysparkling.ml.algos.regression import H2OGAMRegressor
from tests import unit_test_utils

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGAM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGAM")


def testPipelineSerialization(prostateDataset):
    algo = H2OGAM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/gam_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/gam_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/gam_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/gam_pipeline_model"))

    loadedModel.transform(prostateDataset).count()


def testPropagationOfPredictionCol(prostateDataset):
    predictionCol = "my_prediction_col_name"
    algo = H2OGAM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8,
                  predictionCol=predictionCol)

    model = algo.fit(prostateDataset)
    columns = model.transform(prostateDataset).columns
    assert True == (predictionCol in columns)


def testPlugValuesAffectResult(spark, carsDatasetPath):
    carsDataset=spark.read.csv(carsDatasetPath, header=True, inferSchema=True)
    carsDataset=carsDataset.withColumn("economy_20mpg", carsDataset.economy_20mpg.cast("string"))
    [traningDataset, testingDataset] = carsDataset.randomSplit([0.9, 0.1], 1)

    def createInitialGlmDefinition():
        featuresCols=["economy","displacement", "power", "weight", "acceleration", "year", "economy_20mpg"]
        return H2OGAM(featuresCols=featuresCols, labelCol="cylinders", seed=1,splitRatio=0.8)

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
    gam = createInitialGlmDefinition()
    gam.setMissingValuesHandling("PlugValues")
    gam.setPlugValues(plugValues)
    model = gam.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def testInteractionColumnNamesArePassedWithoutException(spark):
    data = [(0.0, "a", 2.0),
            (float("nan"), "b", 8.0),
            (0.0, "a", 4.0),
            (1.0, "b", 1.0)]
    df = spark.createDataFrame(data, ["x", "y", "z"])

    plugValues = {"x": 0, "x_y.a": 1, "x_y.b": 2, "y": "b"}
    gam = H2OGAM(
        labelCol="z",
        seed=42,
        ignoreConstCols=False,
        standardize=False,
        family="gaussian",
        missingValuesHandling="PlugValues",
        plugValues=plugValues)

    gam.fit(df)

def testBetaConstraintsAffectResult(spark, prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)
    featuresCols=["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]

    def createInitialGlmDefinition():
        return H2OGAM(featuresCols=featuresCols, labelCol="CAPSULE", seed=1, splitRatio=0.8)

    referenceGlm = createInitialGlmDefinition()
    referenceModel = referenceGlm.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    betaConstraints = map(lambda feature: (feature, -1000, 1000, 1, 0.2), featuresCols)
    betaConstraintsFrame = spark.createDataFrame(
        betaConstraints,
        ['names', 'lower_bounds', 'upper_bounds', 'beta_given', 'rho'])

    gam = createInitialGlmDefinition()
    gam.setBetaConstraints(betaConstraintsFrame)
    model = gam.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def setParamtersForProblemSpecificTests(gam):
    gam.setLabelCol("CAPSULE")
    gam.setSeed(1),
    gam.setSplitRatio(0.8)
    return gam


def testH2OGAMClassifierBehavesTheSameAsGenericH2OGAMOnStringLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    gam = setParamtersForProblemSpecificTests(H2OGAM())
    referenceModel = gam.fit(trainingDateset.withColumn("CAPSULE", col("CAPSULE").cast("string")))
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParamtersForProblemSpecificTests(H2OGAMClassifier())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OGAMRegressorBehavesTheSameAsGenericH2OGAMOnNumericLabelColumn(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    automl = setParamtersForProblemSpecificTests(H2OGAM())
    referenceModel = automl.fit(trainingDateset)
    referenceDataset = referenceModel.transform(testingDataset)

    classifier = setParamtersForProblemSpecificTests(H2OGAMRegressor())
    model = classifier.fit(trainingDateset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(referenceDataset, result)


def testH2OGAMClassifierBehavesDiffenrentlyThanH2OGAMRegressor(prostateDataset):
    [trainingDateset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)

    regressor = setParamtersForProblemSpecificTests(H2OGAMRegressor())
    regressionModel = regressor.fit(trainingDateset)
    regressionDataset = regressionModel.transform(testingDataset)

    classifier = setParamtersForProblemSpecificTests(H2OGAMClassifier())
    classificationModel = classifier.fit(trainingDateset)
    classificationDataset = classificationModel.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(regressionDataset, classificationDataset)
