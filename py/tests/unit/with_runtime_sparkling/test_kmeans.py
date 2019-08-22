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
Unit tests for PySparkling H2OKMeans
"""
import os
import pytest
from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OKMeans

from tests import unit_test_utils


@pytest.fixture(scope="module")
def dataset(spark):
    return spark.read.csv("file://" + unit_test_utils.locate("smalldata/iris/iris_wheader.csv"),
                          header=True, inferSchema=True)


def testParams():
    kmeans = H2OKMeans(predictionCol="prediction",
                       detailedPredictionCol="detailed_prediction",
                       withDetailedPredictionCol=False,
                       featuresCols=[],
                       foldCol=None,
                       weightCol=None,
                       splitRatio=1.0,
                       seed=-1,
                       nfolds=0,
                       allStringColumnsToCategorical=True,
                       columnsToCategorical=[],
                       convertUnknownCategoricalLevelsToNa=False,
                       convertInvalidNumbersToNa=False,
                       modelId=None,
                       keepCrossValidationPredictions=False,
                       keepCrossValidationFoldAssignment=False,
                       parallelizeCrossValidation=True,
                       distribution="AUTO",
                       maxIterations=10,
                       standardize=True,
                       init="Furthest",
                       userPoints=None,
                       estimateK=False,
                       k=2)

    assert kmeans.getPredictionCol() == "prediction"
    assert kmeans.getDetailedPredictionCol() == "detailed_prediction"
    assert kmeans.getWithDetailedPredictionCol() == False
    assert kmeans.getFeaturesCols() == []
    assert kmeans.getFoldCol() == None
    assert kmeans.getWeightCol() == None
    assert kmeans.getSplitRatio() == 1.0
    assert kmeans.getSeed() == -1
    assert kmeans.getNfolds() == 0
    assert kmeans.getAllStringColumnsToCategorical() == True
    assert kmeans.getColumnsToCategorical() == []
    assert kmeans.getConvertUnknownCategoricalLevelsToNa() == False
    assert kmeans.getConvertInvalidNumbersToNa() == False
    assert kmeans.getModelId() == None
    assert kmeans.getKeepCrossValidationPredictions() == False
    assert kmeans.getKeepCrossValidationFoldAssignment() == False
    assert kmeans.getParallelizeCrossValidation() == True
    assert kmeans.getDistribution() == "AUTO"
    assert kmeans.getMaxIterations() == 10
    assert kmeans.getStandardize() == True
    assert kmeans.getInit() == "Furthest"
    assert kmeans.getUserPoints() == None
    assert kmeans.getEstimateK() == False
    assert kmeans.getK() == 2


def testPipelineSerialization(dataset):
    algo = H2OKMeans(splitRatio=0.8,
                     seed=1,
                     k=3,
                     featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"])

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/kmeans_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/kmeans_pipeline"))
    model = loadedPipeline.fit(dataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/kmeans_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/kmeans_pipeline_model"))

    loadedModel.transform(dataset).count()


def testResultInPredictionCol(dataset):
    algo = H2OKMeans(splitRatio=0.8,
                     seed=1,
                     k=3,
                     featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"])

    model = algo.fit(dataset)
    transformed = model.transform(dataset)
    assert transformed.select("prediction").head()[0] == 0, "Prediction should match"
    assert transformed.select("prediction").distinct().count() == 3, "Number of clusters should match"


def testFullResultInPredictionDetailsCol(dataset):
    algo = H2OKMeans(splitRatio=0.8,
                     seed=1,
                     k=3,
                     featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"],
                     withDetailedPredictionCol=True)

    model = algo.fit(dataset)
    transformed = model.transform(dataset)
    assert transformed.select("detailed_prediction.cluster").head()[0] == 0, "Prediction should match"
    assert len(
        transformed.select("detailed_prediction.distances").head()[0]) == 3, "Size of distances array should match"


def testUserPoints(dataset):
    algo = H2OKMeans(splitRatio=0.8,
                     seed=1,
                     k=3,
                     featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"],
                     userPoints=[[4.9, 3.0, 1.4, 0.2], [5.6, 2.5, 3.9, 1.1], [6.5, 3.0, 5.2, 2.0]])

    model = algo.fit(dataset)
    assert model.transform(dataset).select("prediction").head()[0] == 0, "Prediction should match"
