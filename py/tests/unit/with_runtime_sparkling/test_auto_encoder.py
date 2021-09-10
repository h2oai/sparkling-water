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
from pyspark.ml.linalg import DenseMatrix, DenseVector
from pyspark.sql.types import *
from pysparkling.ml import H2OMOJOModel, H2OAutoEncoder, H2OGBM
from tests import unit_test_utils
from pyspark.ml import Pipeline, PipelineModel

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OAutoEncoder", skip=["mseCol"])


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OAutoEncoder", skip=["mseCol"])


@pytest.mark.skipif(pyspark.__version__.startswith("2.1"), reason="""Support for Spark 2.1 will be removed in SW 3.34. 
Tests are ignored due to a bug in Vector comparison in Spark 2.1: https://issues.apache.org/jira/browse/SPARK-19425""")
def testInitialBiasAndWeightsAffectResult(prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)

    def createInitialAutoEncoderDefinition():
        return H2OAutoEncoder(
            seed=42,
            reproducible=True,
            inputCols=["AGE", "RACE", "DPROS", "DCAPS"],
            hidden=[3, ])

    referenceAutoEncoder = createInitialAutoEncoderDefinition()
    referenceModel = referenceAutoEncoder.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    autoEncoder = createInitialAutoEncoderDefinition()
    matrix0 = DenseMatrix(3, 4, [.1, .2, .3, .4, .4, .5, .6, .7, .7, .8, .9, .6], False)
    matrix1 = DenseMatrix(4, 3, [.11, .21, .32, .42, .44, .53, .63, .71, .72, .82, .912, .62], False)
    autoEncoder.setInitialWeights([matrix0, matrix1])
    autoEncoder.setInitialBiases([DenseVector([.1, .2, .3]), DenseVector([.13, .22, .31, .12])])
    model = autoEncoder.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def testUsageOfAutoEncoderInAPipeline(prostateDataset):
    autoEncoder = H2OAutoEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setHidden([3,]) \
        .setReproducible(True) \
        .setSeed(1)

    gbm = H2OGBM() \
        .setFeaturesCols([autoEncoder.getOutputCol()]) \
        .setLabelCol("CAPSULE")

    pipeline = Pipeline(stages = [autoEncoder, gbm])

    model = pipeline.fit(prostateDataset)
    assert model.transform(prostateDataset).groupBy("prediction").count().count() > 1


@pytest.mark.skipif(pyspark.__version__.startswith("2.1"), reason="""Support for Spark 2.1 will be removed in SW 3.34. 
Tests are ignored due to a bug in Vector comparison in Spark 2.1: https://issues.apache.org/jira/browse/SPARK-19425""")
def testAutoEncoderPipelineSerialization(prostateDataset):
    autoEncoder = H2OAutoEncoder(inputCols=["DPROS", "DCAPS", "RACE", "GLEASON"],
                                 outputCol="output",
                                 originalCol="original",
                                 withOriginalCol=True,
                                 mseCol="mse",
                                 withMSECol=True,
                                 seed=1,
                                 splitRatio=0.8,
                                 reproducible=True)

    pipeline = Pipeline(stages=[autoEncoder])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/autoEncoder_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/autoEncoder_pipeline"))
    model = loadedPipeline.fit(prostateDataset)
    expectedResult = model.transform(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/autoEncoder_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/autoEncoder_pipeline_model"))
    result = loadedModel.transform(prostateDataset)

    unit_test_utils.assert_data_frames_are_identical(expectedResult, result)


def testScoringWithOldMOJO(discourdDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_auto_encoder.mojo"))
    mojo.setOutputCol("Output")
    mojo.setOriginalCol("Original")
    mojo.setWithOriginalCol(True)
    mojo.setMSECol("MSE")
    mojo.setWithMSECol(True)


    firstRow = mojo.transform(discourdDataset).first()

    assert len(firstRow["Output"]) == 210
    assert len(firstRow["Original"]) == 210
    assert 0.01 < firstRow["MSE"] < 0.1
