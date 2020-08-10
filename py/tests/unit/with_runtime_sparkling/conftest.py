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
import os
import pytest
from pyspark.sql import SparkSession
from pysparkling.conf import H2OConf
from pysparkling.context import H2OContext

from tests import unit_test_utils


@pytest.fixture(scope="module")
def spark(spark_conf):
    conf = unit_test_utils.get_default_spark_conf(spark_conf)
    return SparkSession.builder.config(conf=conf).getOrCreate()


@pytest.fixture(scope="module")
def hc(spark):
    return H2OContext.getOrCreate(H2OConf().setClusterSize(1))


@pytest.fixture(scope="module")
def prostateDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/prostate/prostate.csv")


@pytest.fixture(scope="module")
def loanDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/loan.csv")


@pytest.fixture(scope="module")
def insuranceDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/insurance.csv")


@pytest.fixture(scope="module")
def semiconductorDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/semiconductor.csv")


@pytest.fixture(scope="module")
def irisDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/iris/iris_wheader.csv")


@pytest.fixture(scope="module")
def carsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/cars_20mpg.csv")


@pytest.fixture(scope="module")
def arrestsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/USArrests.csv")


@pytest.fixture(scope="module")
def birdsDatasetPath():
    return "file://" + os.path.abspath("../examples/smalldata/birds.csv")


@pytest.fixture(scope="module")
def prostateDataset(spark, prostateDatasetPath):
    return spark.read.csv(prostateDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def semiconductorDataset(spark, semiconductorDatasetPath):
    return spark.read.csv(semiconductorDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def arrestsDataset(spark, arrestsDatasetPath):
    return spark.read.csv(arrestsDatasetPath, header=True, inferSchema=True)


@pytest.fixture(scope="module")
def birdsDataset(spark, birdsDatasetPath):
    return spark.read.csv(birdsDatasetPath, header=True, inferSchema=True)
