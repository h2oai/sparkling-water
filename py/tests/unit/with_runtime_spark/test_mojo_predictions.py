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
Unit tests for PySparkling Mojo. We don't start H2O context for these tests to actually tests
that mojo can run without H2O runtime in PySparkling environment
"""

import os
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import Row
from pysparkling.ml import H2OMOJOModel, H2OMOJOSettings

from tests import unit_test_utils


def testMojoPredictions(spark):
    # Try loading the Mojo and prediction on it without starting H2O Context
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    prostateFrame = spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                   header=True)
    mojo.transform(prostateFrame).repartition(1).collect()


def testMojoPredictionsUnseenCategoricals(spark):
    path = "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_airlines_categoricals.zip")
    settings = H2OMOJOSettings(convertUnknownCategoricalLevelsToNa=True)
    mojo = H2OMOJOModel.createFromMojo(path, settings)

    rowForScoring = Row("sepal_len", "sepal_wid", "petal_len", "petal_wid", "class")

    df = spark.createDataFrame(spark.sparkContext.
                               parallelize([(5.1, 3.5, 1.4, 0.2, "Missing_categorical")]).
                               map(lambda r: rowForScoring(*r)))
    data = mojo.transform(df).collect()[0]

    assert data["class"] == "Missing_categorical"
    assert data["petal_len"] == 1.4
    assert data["petal_wid"] == 0.2
    assert data["sepal_len"] == 5.1
    assert data["sepal_wid"] == 3.5
    assert data["prediction"][0] == 5.240174068202646


def testMojoModelSerializationInPipeline(spark):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    prostateFrame = spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                   header=True)

    pipeline = Pipeline(stages=[mojo])

    pipeline.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))

    model = loadedPipeline.fit(prostateFrame)

    model.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))
    PipelineModel.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))


def testMojoUnsupervised(spark):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/isolation_forest.mojo"))

    rowForScoring = Row("V1")

    df = spark.createDataFrame(spark.sparkContext.
                               parallelize([(5.1,)]).
                               map(lambda r: rowForScoring(*r)))
    mojo.transform(df).repartition(1).collect()
