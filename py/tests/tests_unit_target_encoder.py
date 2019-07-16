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
import os
import unittest

import generic_test_utils
import unit_test_utils
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pysparkling.ml.features import H2OTargetEncoder


class H2OTargetEncoderTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()
        cls._trainingDF = cls._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"))

    def test_targetEncoderParamsGetPropagatedToMOJOModel(self):
        targetEncoder = H2OTargetEncoder(foldCol="ID", labelCol="CAPSULE", inputCols=["RACE", "DPROS" ,"DCAPS"], houldoutStragegy = "KFold",
                                         blendedAvgEnabled=True, blendedAvgInflectionPoint=15.0, blendedAvgSmoothing=25.0, noise=0.05, noiseSeed=123)
        pipeline = Pipeline(stages=[targetEncoder])
        model = pipeline.fit(self._trainingDF)
        path = "file://" + os.path.abspath("build/test_spark_pipeline_model_mojo")
        model.write().overwrite().save(path)
        loadedModel = PipelineModel.load(path)
        mojoModel = loadedModel.stages[0]

        assert targetEncoder.getFoldCol() == mojoModel.getFoldCol()
        assert targetEncoder.getLabelCol() == mojoModel.getLabelCol()
        assert targetEncoder.getInputCols() == mojoModel.getInputCols()
        assert targetEncoder.getHouldoutStrategy() == mojoModel.getHoldouStrategy()
        assert targetEncoder.getBlendedAvgEnabled() == mojoModel.getBlendedAvgEnabled()
        assert targetEncoder.getBlendedAvgInflectionPoint() == mojoModel.getBlendedAvgInflectionPoint()
        assert targetEncoder.getBlendedAvgSmoothing() == mojoModel.getBlendedAvgSmoothing()
        assert targetEncoder.getNoise() == mojoModel.getNoise()
        assert targetEncoder.getNoiseSeed() == mojoModel.getNoiseSeed()
