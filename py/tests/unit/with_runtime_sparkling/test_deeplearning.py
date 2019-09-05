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
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OMOJOModel, H2ODeepLearning

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2ODeepLearning")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2ODeepLearning")


def testLoadAndTrainMojo(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_prostate.mojo"))

    dl = H2ODeepLearning(seed=42, reproducible=True, labelCol="CAPSULE")

    model = dl.fit(prostateDataset)

    predMojo = mojo.transform(prostateDataset).repartition(1).collect()
    predModel = model.transform(prostateDataset).repartition(1).collect()

    assert len(predMojo) == len(predModel)
    for i in range(0, len(predMojo)):
        assert predMojo[i] == predModel[i]
