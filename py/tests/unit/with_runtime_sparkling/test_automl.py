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

from pysparkling.ml import H2OAutoML

from tests import unit_test_utils
from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OAutoML")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OAutoML")


@pytest.fixture(scope="module")
def dataset(spark):
    return spark \
        .read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"), header=True, inferSchema=True) \
        .withColumn("CAPSULE", col("CAPSULE").cast("string"))


def getAlgorithmForGetLeaderboardTesting():
    automl = H2OAutoML(labelCol="CAPSULE", ignoredCols=["ID"])
    automl.setExcludeAlgos(["GLM"])
    automl.setMaxModels(5)
    automl.setSortMetric("AUC")
    return automl


def testGetLeaderboardWithListAsArgument(dataset):
    automl = getAlgorithmForGetLeaderboardTesting()
    automl.fit(dataset)
    extraColumns = ["training_time_ms", "predict_time_per_row_ms"]
    assert automl.getLeaderboard(extraColumns).columns == automl.getLeaderboard().columns + extraColumns


def testGetLeaderboardWithVariableArgumens(dataset):
    automl = getAlgorithmForGetLeaderboardTesting()
    automl.fit(dataset)
    extraColumns = ["training_time_ms", "predict_time_per_row_ms"]
    result = automl.getLeaderboard("training_time_ms", "predict_time_per_row_ms").columns
    expected = automl.getLeaderboard().columns + extraColumns
    assert result == expected
