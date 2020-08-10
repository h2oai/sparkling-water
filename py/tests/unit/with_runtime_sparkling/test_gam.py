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
    algo = H2OGAM(featuresCols=["DPROS", "DCAPS", "RACE", "GLEASON"],
                  gamCols=["PSA", "AGE"],
                  labelCol="CAPSULE",
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
    algo = H2OGAM(featuresCols=["DPROS", "DCAPS", "RACE", "GLEASON"],
                  gamCols=["PSA", "AGE"],
                  labelCol="CAPSULE",
                  seed=1,
                  splitRatio=0.8,
                  predictionCol=predictionCol)

    model = algo.fit(prostateDataset)
    columns = model.transform(prostateDataset).columns
    assert True == (predictionCol in columns)


def testBetaConstraintsAffectResult(spark, prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)
    featuresCols=["DPROS", "DCAPS", "RACE", "GLEASON"]

    def createInitialGamDefinition():
        return H2OGAM(featuresCols=featuresCols, labelCol="CAPSULE", seed=1, splitRatio=0.8, gamCols=["PSA", "AGE"])

    referenceGam = createInitialGamDefinition()
    referenceModel = referenceGam.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    betaConstraints = map(lambda feature: (feature, -1000, 1000, 1, 0.2), featuresCols)
    betaConstraintsFrame = spark.createDataFrame(
        betaConstraints,
        ['names', 'lower_bounds', 'upper_bounds', 'beta_given', 'rho'])

    gam = createInitialGamDefinition()
    gam.setBetaConstraints(betaConstraintsFrame)
    model = gam.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def setParamtersForProblemSpecificTests(gam):
    gam.setLabelCol("CAPSULE")
    gam.setSeed(1)
    gam.setFeaturesCols(["DPROS", "DCAPS", "RACE", "GLEASON"])
    gam.setGamCols(["PSA", "AGE"])
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
    regressionDataset = regressionModel.transform(testingDataset).drop("detailed_prediction")

    classifier = setParamtersForProblemSpecificTests(H2OGAMClassifier())
    classificationModel = classifier.fit(trainingDateset)
    classificationDataset = classificationModel.transform(testingDataset).drop("detailed_prediction")

    unit_test_utils.assert_data_frames_have_different_values(regressionDataset, classificationDataset)
