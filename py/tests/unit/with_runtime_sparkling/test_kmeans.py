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
from tests.unit.with_runtime_sparkling.algo_test_utils import *


@pytest.fixture(scope="module")
def dataset(spark):
    return spark.read.csv("file://" + unit_test_utils.locate("smalldata/iris/iris_wheader.csv"),
                          header=True, inferSchema=True)


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OKMeans")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OKMeans")


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
