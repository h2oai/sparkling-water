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
import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os
from pysparkling.ml import H2OMOJOModel

import unit_test_utils
import generic_test_utils
from pyspark.ml import Pipeline, PipelineModel


#
# These tests does not start H2O Context on purpose to test running predictions
# in Spark environment without run-time H2O
#
class H2OMojoPredictionsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()

    # test predictions on H2O Mojo
    def test_h2o_mojo_predictions(self):
        # Try loading the Mojo and prediction on it without starting H2O Context
        mojo = H2OMOJOModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)
        mojo.transform(prostate_frame).repartition(1).collect()

    def test_h2o_mojo_predictions_unseen_categoricals(self):
        mojo = H2OMOJOModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_airlines_categoricals.zip"))
        mojo.setConvertUnknownCategoricalLevelsToNa(True)
        row_for_scoring = Row("sepal_len", "sepal_wid", "petal_len", "petal_wid", "class")

        df = self._spark.createDataFrame(self._spark.sparkContext.
                                         parallelize([(5.1, 3.5, 1.4, 0.2, "Missing_categorical")]).
                                         map(lambda r: row_for_scoring(*r)))
        data = mojo.transform(df).collect()[0]

        assert data["class"] == "Missing_categorical"
        assert data["petal_len"] == 1.4
        assert data["petal_wid"] == 0.2
        assert data["sepal_len"] == 5.1
        assert data["sepal_wid"] == 3.5
        assert data["prediction"][0] == 5.240174068202646

    def test_h2o_mojo_model_serialization_in_pipeline(self):
        mojo = H2OMOJOModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
        prostate_frame = self._spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                                              header=True)

        pipeline = Pipeline(stages=[mojo])

        pipeline.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))
        loaded_pipeline = Pipeline.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))

        model = loaded_pipeline.fit(prostate_frame)

        model.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))
        PipelineModel.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))

    def test_h2o_mojo_unsupervised(self):
        mojo = H2OMOJOModel.createFromMojo(
            "file://" + os.path.abspath("../ml/src/test/resources/isolation_forest.mojo"))

        row_for_scoring = Row("V1")

        df = self._spark.createDataFrame(self._spark.sparkContext.
                                         parallelize([(5.1,)]).
                                         map(lambda r: row_for_scoring(*r)))
        mojo.transform(df).repartition(1).collect()


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OMojoPredictionsTest], file_name="py_unit_tests_mojo_predictions_report")
