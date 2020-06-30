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

import pytest

from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OGBM, H2OMOJOModel, H2OSupervisedMOJOModel, H2OTreeBasedSupervisedMOJOModel


@pytest.fixture(scope="module")
def gbmModel(prostateDataset):
    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")
    return gbm.fit(prostateDataset)

def testDomainColumns(gbmModel):
    domainValues = gbmModel.getDomainValues()
    assert domainValues["DPROS"] is None
    assert domainValues["DCAPS"] is None
    assert domainValues["VOL"] is None
    assert domainValues["AGE"] is None
    assert domainValues["PSA"] is None
    assert domainValues["capsule"] == ["0", "1"]
    assert domainValues["RACE"] is None
    assert domainValues["ID"] is None

def testTrainingParams(gbmModel):
    params = gbmModel.getTrainingParams()
    assert params["seed"] == "42"
    assert params["distribution"] == "bernoulli"
    assert params["ntrees"] == "2"
    assert len(params) == 46

def testModelCategory(gbmModel):
    category = gbmModel.getModelCategory()
    assert category == "Binomial"

def testTrainingMetrics(gbmModel):
    metrics = gbmModel.getTrainingMetrics()
    assert metrics is not None
    assert len(metrics) is 6

def getCurrentMetrics():
    metrics = gbmModel.getCurrentMetrics()
    assert metrics == gbmModel.getTrainingMetrics()
