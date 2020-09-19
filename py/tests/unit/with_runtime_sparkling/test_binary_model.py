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

import h2o
from h2o.estimators.estimator_base import H2OEstimator
from pyspark.mllib.linalg import *
from pysparkling.ml import *


def testModelIsLoadedOnBackend(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)
    algo.fit(prostateDataset)
    algo.getBinaryModel().write("build/binary.model")
    swBinaryModel = H2OBinaryModel.read("build/binary.model")
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLM(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLMClassifier(prostateDataset):
    algo = H2OGLMClassifier(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                            labelCol="AGE",
                            seed=1,
                            splitRatio=0.8)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLMRegressor(prostateDataset):
    algo = H2OGLMRegressor(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                           labelCol="AGE",
                           seed=1,
                           splitRatio=0.8)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)
