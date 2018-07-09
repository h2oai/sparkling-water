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


class H2OMojoPipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cloud_name = generic_test_utils.unique_cloud_name("h2o_mojo_predictions_test")
        cls._spark = SparkSession.builder.config(conf=unit_test_utils.get_default_spark_conf()).getOrCreate()

    # test predictions on H2O Pipeline MOJO
    def test_h2o_mojo_pipeline_predictions(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOPipelineModel.create_from_mojo(
            "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        preds = mojo.predict(prostate_frame).repartition(1)

        normalSelection = preds.select("prediction.preds").take(5)

        assert normalSelection[0][0][0] == 65.36320409515132
        assert normalSelection[1][0][0] == 64.96902128114817
        assert normalSelection[2][0][0] == 64.96721023747583
        assert normalSelection[3][0][0] == 65.78772654671035
        assert normalSelection[4][0][0] == 66.11327967814829

        udfSelection = preds.select(mojo.select_prediction_udf("AGE")).take(5)

        assert udfSelection[0][0] == 65.36320409515132
        assert udfSelection[1][0] == 64.96902128114817
        assert udfSelection[2][0] == 64.96721023747583
        assert udfSelection[3][0] == 65.78772654671035
        assert udfSelection[4][0] == 66.11327967814829

    # test predictions on H2O Pipeline MOJO
    def test_h2o_mojo_pipeline_predictions_with_named_cols(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOPipelineModel.create_from_mojo(
            "file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        mojo.set_named_mojo_output_columns(True)
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        preds = mojo.predict(prostate_frame).repartition(1).select(mojo.select_prediction_udf("AGE")).take(5)

        assert preds[0][0] == 65.36320409515132
        assert preds[1][0] == 64.96902128114817
        assert preds[2][0] == 64.96721023747583
        assert preds[3][0] == 65.78772654671035
        assert preds[4][0] == 66.11327967814829


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OMojoPipelineTest], file_name="py_unit_tests_mojo_pipeline_report")
