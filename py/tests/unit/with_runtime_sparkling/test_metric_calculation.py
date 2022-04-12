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
from pysparkling.ml import *


def testRegressionMetricsCalculation(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/regre_model_prostate.mojo"))
    metrics = H2ORegressionMetrics.calculate(mojo.transform(prostateDataset), labelCol = "CAPSULE")
    assert metrics is not None
    assert metrics.getMAE() > 0.0
    assert metrics.getRMSLE() > 0.0


def testBinomialMetricsCalculation(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    domain = mojo.getDomainValues()["capsule"]
    metrics = H2OBinomialMetrics.calculate(mojo.transform(prostateDataset), domain, labelCol = "CAPSULE")
    assert metrics is not None
    assert metrics.getAUC() > 0.5
    assert metrics.getConfusionMatrix().count() > 0


def testMultinomialMetricsCalculation(irisDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/multi_model_iris.mojo"))
    domain = mojo.getDomainValues()["class"]
    metrics = H2OMultinomialMetrics.calculate(mojo.transform(irisDataset), domain, labelCol = "class")
    assert metrics is not None
    assert metrics.getLogloss() > 0.0
    assert metrics.getConfusionMatrix().count() > 0
