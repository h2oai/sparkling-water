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
from pysparkling.ml import H2OPCAMOJOModel, H2OAutoEncoder, H2OPCA, H2OGBM
from tests import unit_test_utils
from pyspark.ml import Pipeline, PipelineModel

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OPCA")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OPCA")


def testUsageOfPCAInAPipeline(prostateDataset):

    pca = H2OPCA() \
        .setInputCols(["RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]) \
        .setK(3) \
        .setImputeMissing(True) \
        .setSeed(42)

    gbm = H2OGBM() \
        .setFeaturesCols([pca.getOutputCol()]) \
        .setLabelCol("CAPSULE") \
        .setSeed(42)

    pipeline = Pipeline(stages=[pca, gbm])

    model = pipeline.fit(prostateDataset)
    assert model.transform(prostateDataset).groupBy("prediction").count().count() > 1


def testPCAPipelineSerialization(prostateDataset):
    pca = H2OPCA(inputCols=["DPROS", "DCAPS", "RACE", "GLEASON", "VOL"],
                         outputCol="output",
                         seed=1,
                         splitRatio=0.8,
                         k=3)

    pipeline = Pipeline(stages=[pca])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/pca_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/pca_pipeline"))
    model = loadedPipeline.fit(prostateDataset)
    expectedResult = model.transform(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/pca_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/pca_pipeline_model"))
    result = loadedModel.transform(prostateDataset)

    unit_test_utils.assert_data_frames_are_identical(expectedResult, result)


def testScoringWithOldMOJO(prostateDataset):
    mojo = H2OPCAMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/pca_prostate.mojo"))
    mojo.setOutputCol("Output")

    firstRow = mojo.transform(prostateDataset).first()
    print(firstRow)

    assert len(firstRow["Output"]) == 3
    assert 2.2981 < firstRow["Output"][0] < 2.2982
    assert -0.744 < firstRow["Output"][1] < -0.743
    assert -6.1720 < firstRow["Output"][2] < -6.1719
