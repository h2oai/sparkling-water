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
Unit tests for PySparkling Mojo. We don't start H2O context for these tests to actually tests
that mojo can run without H2O runtime in PySparkling environment
"""

import unittest
from pyspark.sql import SparkSession

import os
from pysparkling.ml import H2OMOJOModel, H2OMOJOPipelineModel

import unit_test_utils
import generic_test_utils


class H2OMojoPredictionsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cloud_name = generic_test_utils.unique_cloud_name("h2o_mojo_predictions_test")
        cls._spark = SparkSession.builder.config(conf = unit_test_utils.get_default_spark_conf()).getOrCreate()

    # test predictions on H2O Mojo
    def test_h2o_mojo_predictions(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOModel.create_from_mojo("file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"), header=True)
        mojo.predict(prostate_frame).repartition(1).collect()

    def test_h2o_mojo_predictions_unseen_categoricals(self):
        mojo = H2OMOJOModel.create_from_mojo("file://" + os.path.abspath("../ml/src/test/resources/deep_learning_airlines_categoricals.zip"))
        mojo.setConvertUnknownCategoricalLevelsToNa(True)
        d =[{'sepal_len':5.1, 'sepal_wid':3.5, 'petal_len':1.4, 'petal_wid':0.2, 'class':'Missing_categorical'}]
        df = self._spark.createDataFrame(d)
        data = mojo.transform(df).collect()[0]
        assert data["class"] == "Missing_categorical"
        assert data["petal_len"] == 1.4
        assert data["petal_wid"] == 0.2
        assert data["sepal_len"] == 5.1
        assert data["sepal_wid"] == 3.5
        assert data["prediction_output"][0] == 5.240174068202646

    # test predictions on H2O Pipeline MOJO
    def test_h2o_mojo_pipeline_predictions(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOPipelineModel.create_from_mojo("file://" + os.path.abspath("../ml/src/test/resources/mojo2data/pipeline.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"), header=True)
        preds = mojo.predict(prostate_frame).repartition(1).select("prediction.preds").take(5)

        assert preds[0][0][0] == "65.36320409515132"
        assert preds[1][0][0] == "64.96902128114817"
        assert preds[2][0][0] == "64.96721023747583"
        assert preds[3][0][0] == "65.78772654671035"
        assert preds[4][0][0] == "66.11327967814829"

if __name__ == '__main__':
    generic_test_utils.run_tests([H2OMojoPredictionsTest], file_name="py_unit_tests_mojo_predictions_report")
