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

import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest

import generic_test_utils
import unit_test_utils
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pysparkling.ml import H2OTargetEncoder, H2OGBM


class H2OTargetEncoderTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()
        dataset = cls._spark.read\
            .options(header='true', inferSchema='true')\
            .csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"))
        [cls._trainingDataset, cls._testingDataset] = dataset.randomSplit([0.8, 0.2], 1)


    def assertTargetEncoderAndMOJOModelParamsAreEqual(self, targetEncoder, mojoModel):
        assert targetEncoder.getFoldCol() == mojoModel.getFoldCol()
        assert targetEncoder.getLabelCol() == mojoModel.getLabelCol()
        assert targetEncoder.getInputCols() == mojoModel.getInputCols()
        assert targetEncoder.getHoldoutStrategy() == mojoModel.getHoldoutStrategy()
        assert targetEncoder.getBlendedAvgEnabled() == mojoModel.getBlendedAvgEnabled()
        assert targetEncoder.getBlendedAvgInflectionPoint() == mojoModel.getBlendedAvgInflectionPoint()
        assert targetEncoder.getBlendedAvgSmoothing() == mojoModel.getBlendedAvgSmoothing()
        assert targetEncoder.getNoise() == mojoModel.getNoise()
        assert targetEncoder.getNoiseSeed() == mojoModel.getNoiseSeed()


    def testTargetEncoderConstructorParametersGetPropagatedToLoadedMOJOModel(self):
        targetEncoder = H2OTargetEncoder(foldCol="ID", labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"], holdoutStrategy = "KFold",
                                         blendedAvgEnabled=True, blendedAvgInflectionPoint=15.0, blendedAvgSmoothing=25.0, noise=0.05, noiseSeed=123)
        pipeline = Pipeline(stages=[targetEncoder])
        model = pipeline.fit(self._trainingDataset)
        path = "file://" + os.path.abspath("build/testTargetEncoderConstructorParametersGetPropagatedToLoadedMOJOModel")
        model.write().overwrite().save(path)
        loadedModel = PipelineModel.load(path)
        mojoModel = loadedModel.stages[0]

        self.assertTargetEncoderAndMOJOModelParamsAreEqual(targetEncoder, mojoModel)


    def testTargetEncoderSetterParametersGetPropagatedToLoadedMOJOModel(self):
        targetEncoder = H2OTargetEncoder()\
            .setFoldCol("ID")\
            .setLabelCol("CAPSULE")\
            .setInputCols(["RACE", "DPROS", "DCAPS"])\
            .setHoldoutStrategy("KFold")\
            .setBlendedAvgEnabled(True)\
            .setBlendedAvgInflectionPoint(15.0)\
            .setBlendedAvgSmoothing(25.0)\
            .setNoise(0.05)\
            .setNoiseSeed(123)
        pipeline = Pipeline(stages=[targetEncoder])
        model = pipeline.fit(self._trainingDataset)
        path = "file://" + os.path.abspath("build/testTargetEncoderSetterParametersGetPropagatedToLoadedMOJOModel")
        model.write().overwrite().save(path)
        loadedModel = PipelineModel.load(path)
        mojoModel = loadedModel.stages[0]

        self.assertTargetEncoderAndMOJOModelParamsAreEqual(targetEncoder, mojoModel)


    def testPipelineWithTargetEncoderTransformsTrainingAndTestingDatasetWithoutException(self):
        targetEncoder = H2OTargetEncoder(labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"])
        gbm = H2OGBM(labelCol="CAPSULE")

        pipeline = Pipeline(stages=[targetEncoder, gbm])
        model = pipeline.fit(self._trainingDataset)

        model.transform(self._testingDataset).collect()


    def testProducedMOJOModelAndLoadedMOJOModelReturnsSameResult(self):
        targetEncoder = H2OTargetEncoder(labelCol="CAPSULE", inputCols=["RACE", "DPROS", "DCAPS"])
        pipeline = Pipeline(stages=[targetEncoder])
        producedModel = pipeline.fit(self._trainingDataset)
        path = "file://" + os.path.abspath("build/testProducedMOJOModelAndLoadedMOJOModelReturnsSameResult")
        producedModel.write().overwrite().save(path)
        loadedModel = PipelineModel.load(path)

        transformedByProducedModel = producedModel.transform(self._testingDataset)
        transformedByLoadedModel = loadedModel.transform(self._testingDataset)

        unit_test_utils.assert_data_frames_are_identical(transformedByProducedModel, transformedByLoadedModel)

    def testTargetEncoderModelWithDisabledNoiseAndTargetEncoderMOJOModelTransformTheTrainingDatasetSameWay(self):
        targetEncoder = H2OTargetEncoder()\
            .setInputCols(["RACE", "DPROS", "DCAPS"])\
            .setLabelCol("CAPSULE")\
            .setHoldoutStrategy("None")\
            .setNoise(0.0)
        targetEncoderModel = targetEncoder.fit(self._trainingDataset)

        transformedByModel = targetEncoderModel.transformTrainingDataset(self._trainingDataset)
        transformedByMOJOModel = targetEncoderModel.transform(self._trainingDataset)

        unit_test_utils.assert_data_frames_are_identical(transformedByModel, transformedByMOJOModel)


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OTargetEncoderTestSuite], file_name="py_unit_tests_target_encoder_report")
