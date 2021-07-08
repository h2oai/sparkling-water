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
import pyspark
from pyspark.sql.types import *
from pysparkling.ml import H2OGLRMMOJOModel, H2OAutoEncoder, H2OGLRM, H2OGBM
from tests import unit_test_utils
from pyspark.ml import Pipeline, PipelineModel

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGLRM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGLRM")


def testUsageOfGLRMInAPipeline(prostateDataset):

    pca = H2OGLRM() \
        .setInputCols(["RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]) \
        .setK(3)

    gbm = H2OGBM() \
        .setFeaturesCols([pca.getOutputCol()]) \
        .setLabelCol("CAPSULE")

    pipeline = Pipeline(stages=[pca, gbm])

    model = pipeline.fit(prostateDataset)
    assert model.transform(prostateDataset).groupBy("prediction").count().count() > 1