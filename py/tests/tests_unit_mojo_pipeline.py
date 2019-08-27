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

"""
Unit tests for MOJO pipelines functionality in PySparkling. We don't start H2O context for these tests to actually tests
that mojo can run without H2O runtime in PySparkling environment
"""
import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import os
import unittest

from tests import  generic_test_utils, unit_test_utils
from pyspark.sql import SparkSession
from pysparkling.ml import H2OMOJOPipelineModel, H2OMOJOSettings
from pyspark.ml import Pipeline, PipelineModel


#
# These tests does not start H2O Context on purpose to test running predictions
# in Spark environment without run-time H2O
#
class H2OMojoPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()

    # test predictions on H2O Pipeline MOJO
    def test_h2o_mojo_pipeline_predictions(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        path = "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo")
        settings = H2OMOJOSettings(namedMojoOutputColumns=False)
        mojo = H2OMOJOPipelineModel.createFromMojo(path, settings)

        prostateFrame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        preds = mojo.transform(prostateFrame).repartition(1)

        normalSelection = preds.select("prediction.preds").take(5)

        assert normalSelection[0][0][0] == 65.36320409515132
        assert normalSelection[1][0][0] == 64.96902128114817
        assert normalSelection[2][0][0] == 64.96721023747583
        assert normalSelection[3][0][0] == 65.78772654671035
        assert normalSelection[4][0][0] == 66.11327967814829

        udfSelection = preds.select(mojo.selectPredictionUDF("AGE")).take(5)

        assert udfSelection[0][0] == 65.36320409515132
        assert udfSelection[1][0] == 64.96902128114817
        assert udfSelection[2][0] == 64.96721023747583
        assert udfSelection[3][0] == 65.78772654671035
        assert udfSelection[4][0] == 66.11327967814829

    # test predictions on H2O Pipeline MOJO
    def test_h2o_mojo_pipeline_predictions_with_named_cols(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOPipelineModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        prostateFrame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        preds = mojo.transform(prostateFrame).repartition(1).select(mojo.selectPredictionUDF("AGE")).take(5)

        assert preds[0][0] == 65.36320409515132
        assert preds[1][0] == 64.96902128114817
        assert preds[2][0] == 64.96721023747583
        assert preds[3][0] == 65.78772654671035
        assert preds[4][0] == 66.11327967814829

    def test_mojo_dai_pipeline_serialize(self):
        mojo = H2OMOJOPipelineModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        prostateFrame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        # Create Spark pipeline of single step - mojo pipeline
        pipeline = Pipeline(stages=[mojo])
        pipeline.write().overwrite().save("file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline"))
        loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline"))

        # Train the pipeline model
        model = loadedPipeline.fit(prostateFrame)

        model.write().overwrite().save("file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline_model"))
        loadedModel = PipelineModel.load(
            "file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline_model"))

        preds = loadedModel.transform(prostateFrame).repartition(1).select(mojo.selectPredictionUDF("AGE")).take(5)

        assert preds[0][0] == 65.36320409515132
        assert preds[1][0] == 64.96902128114817
        assert preds[2][0] == 64.96721023747583
        assert preds[3][0] == 65.78772654671035
        assert preds[4][0] == 66.11327967814829

    def testMojoPipelineProtoBackendWithoutError(self):
        mojo = H2OMOJOPipelineModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/proto_based_pipeline.mojo"))

        data = [(2.0,'male',0.41670000553131104,111361,6.449999809265137,'A19'),
             (1.0,'female',0.33329999446868896,110413,6.4375,'A14'),
             (1.0,'female',0.16670000553131104,111320,6.237500190734863,'A21'),
             (1.0,'female',2.0,111361,6.237500190734863,'A20'),
             (3.0,'female',1.0,110152,6.75,'A14'),
             (1.0,'male',0.666700005531311,110489,6.85830020904541,'A10'),
             (3.0,'male',0.33329999446868896,111320,0.0,'A11'),
             (3.0,'male',2.0,110413,6.85830020904541,'A24'),
             (1.0,'female',1.0,110489,3.170799970626831,'A21'),
             (1.0,'female',0.33329999446868896,111240,0.0,'A14')
             ]
        rdd = self._spark.sparkContext.parallelize(data)
        df = self._spark.createDataFrame(rdd, ['pclass', 'sex', 'age', 'ticket', 'fare', 'cabin'])
        prediction = mojo.transform(df)
        prediction.collect()

if __name__ == '__main__':
    generic_test_utils.run_tests([H2OMojoPipelineTest], file_name="py_unit_tests_mojo_pipeline_report")
