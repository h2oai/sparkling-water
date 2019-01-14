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

import os
import unittest

import generic_test_utils
import unit_test_utils
from pyspark.sql import SparkSession

from pysparkling.ml import H2OMOJOPipelineModel
from pyspark.ml import Pipeline, PipelineModel


class H2OMojoPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cloud_name = generic_test_utils.unique_cloud_name("h2o_mojo_predictions_test")
        cls._spark = SparkSession.builder.config(conf=unit_test_utils.get_default_spark_conf()).getOrCreate()


    def test_mojo_dai_pipeline_serialize(self):
        mojo = H2OMOJOPipelineModel.create_from_mojo(
            "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        # Create Spark pipeline of single step - mojo pipeline
        pipeline = Pipeline(stages=[mojo])
        pipeline.write().overwrite().save( "file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline"))
        loaded_pipeline = Pipeline.load( "file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline"))

        ## Train the pipeline model
        model = loaded_pipeline.fit(prostate_frame)

        model.write().overwrite().save( "file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline_model"))
        loaded_model = PipelineModel.load( "file://" + os.path.abspath("build/test_dai_pipeline_as_spark_pipeline_model"))

        preds = loaded_model.transform(prostate_frame).repartition(1).select(mojo.select_prediction_udf("AGE")).take(5)

        assert preds[0][0] == 65.36320409515132
        assert preds[1][0] == 64.96902128114817
        assert preds[2][0] == 64.96721023747583
        assert preds[3][0] == 65.78772654671035
        assert preds[4][0] == 66.11327967814829


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OMojoPipelineTest], file_name="py_unit_tests_mojo_pipeline_report")
