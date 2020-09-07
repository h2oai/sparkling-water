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
from pyspark.ml import Pipeline, PipelineModel
from pysparkling.ml import H2OTargetEncoder, H2OGBM
from ai.h2o.sparkling.ml.models.H2OTargetEncoderMOJOModel import H2OTargetEncoderMOJOModel

from tests import unit_test_utils


@pytest.fixture(scope="module")
def dataset(prostateDataset):
    return prostateDataset.randomSplit([0.8, 0.2], 1)


@pytest.fixture(scope="module")
def trainingDataset(dataset):
    return dataset[0]


@pytest.fixture(scope="module")
def testingDataset(dataset):
    return dataset[1]


def assertTargetEncoderAndMOJOModelParamsAreEqual(expected, produced):
    assert expected.getFoldCol() == produced.getFoldCol()
    assert expected.getLabelCol() == produced.getLabelCol()
    assert expected.getInputCols() == produced.getInputCols()
    assert expected.getOutputCols() == produced.getOutputCols()
    assert expected.getHoldoutStrategy() == produced.getHoldoutStrategy()
    assert expected.getBlendedAvgEnabled() == produced.getBlendedAvgEnabled()
    assert expected.getBlendedAvgInflectionPoint() == produced.getBlendedAvgInflectionPoint()
    assert expected.getBlendedAvgSmoothing() == produced.getBlendedAvgSmoothing()
    assert expected.getNoise() == produced.getNoise()
    assert expected.getNoiseSeed() == produced.getNoiseSeed()


def testTargetEncoderConstructorParametersGetPropagatedToLoadedMOJOModel(trainingDataset):
    targetEncoder = H2OTargetEncoder(foldCol="ID", labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"],
                                     outputCols=["RACE_out", "DPROS_out", "DCAPS_out"], holdoutStrategy="KFold",
                                     blendedAvgEnabled=True, blendedAvgInflectionPoint=15.0, blendedAvgSmoothing=25.0,
                                     noise=0.05, noiseSeed=123)
    pipeline = Pipeline(stages=[targetEncoder])
    model = pipeline.fit(trainingDataset)
    path = "file://" + os.path.abspath("build/testTargetEncoderConstructorParametersGetPropagatedToLoadedMOJOModel")
    model.write().overwrite().save(path)
    loadedModel = PipelineModel.load(path)
    mojoModel = loadedModel.stages[0]

    assertTargetEncoderAndMOJOModelParamsAreEqual(targetEncoder, mojoModel)


def testTargetEncoderMOJOModelCouldBeSavedAndLoaded(trainingDataset, testingDataset):
    targetEncoder = H2OTargetEncoder(foldCol="ID", labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"],
                                     outputCols=["RACE_out", "DPROS_out", "DCAPS_out"])
    model = targetEncoder.fit(trainingDataset)
    path = "file://" + os.path.abspath("build/testTargetEncoderMOJOModelCouldBeSavedAndLoaded")
    model.write().overwrite().save(path)
    loadedModel = H2OTargetEncoderMOJOModel.load(path)

    expected = model.transform(testingDataset)
    result = loadedModel.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testPipelineWithTargetEncoderIsSerializable():
    targetEncoder = H2OTargetEncoder(foldCol="ID", labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"],
                                     outputCols=["RACE_out", "DPROS_out", "DCAPS_out"], holdoutStrategy="KFold",
                                     blendedAvgEnabled=True, blendedAvgInflectionPoint=15.0, blendedAvgSmoothing=25.0,
                                     noise=0.05, noiseSeed=123)
    gbm = H2OGBM() \
        .setLabelCol("CAPSULE") \
        .setFeaturesCols(targetEncoder.getOutputCols())
    pipeline = Pipeline(stages=[targetEncoder, gbm])
    path = "file://" + os.path.abspath("build/testPipelineWithTargetEncoderIsSerializable")
    pipeline.write().overwrite().save(path)
    loadedPipeline = Pipeline.load(path)
    [loadedTargetEncoder, loadedGbm] = loadedPipeline.getStages()

    assertTargetEncoderAndMOJOModelParamsAreEqual(targetEncoder, loadedTargetEncoder)
    assert gbm.getLabelCol() == loadedGbm.getLabelCol()
    assert gbm.getFeaturesCols() == loadedGbm.getFeaturesCols()


def testTargetEncoderSetterParametersGetPropagatedToLoadedMOJOModel(trainingDataset):
    targetEncoder = H2OTargetEncoder() \
        .setFoldCol("ID") \
        .setLabelCol("CAPSULE") \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setOutputCols(["RACE_out", "DPROS_out", "DCAPS_out"]) \
        .setHoldoutStrategy("KFold") \
        .setBlendedAvgEnabled(True) \
        .setBlendedAvgInflectionPoint(15.0) \
        .setBlendedAvgSmoothing(25.0) \
        .setNoise(0.05) \
        .setNoiseSeed(123)
    pipeline = Pipeline(stages=[targetEncoder])
    model = pipeline.fit(trainingDataset)
    path = "file://" + os.path.abspath("build/testTargetEncoderSetterParametersGetPropagatedToLoadedMOJOModel")
    model.write().overwrite().save(path)
    loadedModel = PipelineModel.load(path)
    mojoModel = loadedModel.stages[0]

    assertTargetEncoderAndMOJOModelParamsAreEqual(targetEncoder, mojoModel)


def testPipelineWithTargetEncoderTransformsTrainingAndTestingDatasetWithoutException(trainingDataset, testingDataset):
    targetEncoder = H2OTargetEncoder(labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"])
    gbm = H2OGBM(labelCol="CAPSULE")

    pipeline = Pipeline(stages=[targetEncoder, gbm])
    model = pipeline.fit(trainingDataset)

    model.transform(testingDataset).collect()


def testProducedMOJOModelAndLoadedMOJOModelReturnsSameResult(trainingDataset, testingDataset):
    targetEncoder = H2OTargetEncoder(labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"])
    pipeline = Pipeline(stages=[targetEncoder])
    producedModel = pipeline.fit(trainingDataset)
    path = "file://" + os.path.abspath("build/testProducedMOJOModelAndLoadedMOJOModelReturnsSameResult")
    producedModel.write().overwrite().save(path)
    loadedModel = PipelineModel.load(path)

    transformedByProducedModel = producedModel.transform(testingDataset)
    transformedByLoadedModel = loadedModel.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(transformedByProducedModel, transformedByLoadedModel)


def testTargetEncoderModelWithDisabledNoiseAndTargetEncoderMOJOModelTransformTheTrainingDatasetSameWay(trainingDataset):
    targetEncoder = H2OTargetEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setLabelCol("CAPSULE") \
        .setHoldoutStrategy("None") \
        .setNoise(0.0)
    targetEncoderModel = targetEncoder.fit(trainingDataset)

    transformedByModel = targetEncoderModel.transformTrainingDataset(trainingDataset)
    transformedByMOJOModel = targetEncoderModel.transform(trainingDataset)

    unit_test_utils.assert_data_frames_are_identical(transformedByModel, transformedByMOJOModel)


def testTargetEncoderMOJOModelProduceSameResultsRegardlessSpecificationOfOutputCols(trainingDataset, testingDataset):
    def trainAndReturnTranformedTestingDataset(targetEncoder):
        targetEncoderModel = targetEncoder.fit(trainingDataset)
        return targetEncoderModel.transform(testingDataset)

    targetEncoderDefaultOutputCols = H2OTargetEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setLabelCol("CAPSULE") \
        .setHoldoutStrategy("None") \
        .setNoise(0.0)
    dataFrameDefaultOutputCols = trainAndReturnTranformedTestingDataset(targetEncoderDefaultOutputCols) \
        .withColumnRenamed("RACE_te", "RACE_out") \
        .withColumnRenamed("DPROS_te", "DPROS_out") \
        .withColumnRenamed("DCAPS_te", "DCAPS_out")

    targetEncoderCustomOutputCols = H2OTargetEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setOutputCols(["RACE_out", "DPROS_out", "DCAPS_out"]) \
        .setLabelCol("CAPSULE") \
        .setHoldoutStrategy("None") \
        .setNoise(0.0)
    dataFrameCustomOutputCols = trainAndReturnTranformedTestingDataset(targetEncoderCustomOutputCols)

    unit_test_utils.assert_data_frames_are_identical(dataFrameDefaultOutputCols, dataFrameCustomOutputCols)


def testTargetEncoderModelProduceSameResultsRegardlessSpecificationOfOutputCols(trainingDataset, testingDataset):
    def trainAndReturnTranformedTestingDataset(targetEncoder):
        targetEncoderModel = targetEncoder.fit(trainingDataset)
        return targetEncoderModel.transformTrainingDataset(testingDataset)

    targetEncoderDefaultOutputCols = H2OTargetEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setLabelCol("CAPSULE") \
        .setHoldoutStrategy("None") \
        .setNoise(0.0)
    dataFrameDefaultOutputCols = trainAndReturnTranformedTestingDataset(targetEncoderDefaultOutputCols) \
        .withColumnRenamed("RACE_te", "RACE_out") \
        .withColumnRenamed("DPROS_te", "DPROS_out") \
        .withColumnRenamed("DCAPS_te", "DCAPS_out")

    targetEncoderCustomOutputCols = H2OTargetEncoder() \
        .setInputCols(["RACE", "DPROS", "DCAPS"]) \
        .setOutputCols(["RACE_out", "DPROS_out", "DCAPS_out"]) \
        .setLabelCol("CAPSULE") \
        .setHoldoutStrategy("None") \
        .setNoise(0.0)
    dataFrameCustomOutputCols = trainAndReturnTranformedTestingDataset(targetEncoderCustomOutputCols)

    unit_test_utils.assert_data_frames_are_identical(dataFrameDefaultOutputCols, dataFrameCustomOutputCols)
