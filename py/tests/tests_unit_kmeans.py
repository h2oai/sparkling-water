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
Unit tests for PySparkling H2OKMeans
"""
import os
import sys

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf
from pysparkling.ml import H2OKMeans
from pyspark.sql import SparkSession
import unit_test_utils
import generic_test_utils
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.ml import Pipeline, PipelineModel


class H2OKMeansTestSuite(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()
        cls._hc = H2OContext.getOrCreate(cls._spark, H2OConf(cls._spark).set_cluster_size(1))
        cls.dataset = cls._spark.read.csv("file://" + unit_test_utils.locate("smalldata/iris/iris_wheader.csv"),
                                          header=True, inferSchema=True)

    def testPipelineSerialization(self):
        algo = H2OKMeans(splitRatio=0.8,
                         seed=1,
                         k=3,
                         featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"])

        pipeline = Pipeline(stages=[algo])
        pipeline.write().overwrite().save("file://" + os.path.abspath("build/kmeans_pipeline"))
        loaded_pipeline = Pipeline.load("file://" + os.path.abspath("build/kmeans_pipeline"))
        model = loaded_pipeline.fit(self.dataset)

        model.write().overwrite().save("file://" + os.path.abspath("build/kmeans_pipeline_model"))
        loadedModel = PipelineModel.load("file://" + os.path.abspath("build/kmeans_pipeline_model"))

        loadedModel.transform(self.dataset).count()

    def testResultInPredictionCol(self):
        algo = H2OKMeans(splitRatio=0.8,
                         seed=1,
                         k=3,
                         featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"])

        model = algo.fit(self.dataset)
        transformed = model.transform(self.dataset)
        self.assertEquals(transformed.select("prediction").head()[0], 0, "Prediction should match")
        self.assertEquals(transformed.select("prediction").distinct().count(), 3, "Number of clusters should match")

    def testFullResultInPredictionDetailsCol(self):
        algo = H2OKMeans(splitRatio=0.8,
                         seed=1,
                         k=3,
                         featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"],
                         withDetailedPredictionCol=True)

        model = algo.fit(self.dataset)
        transformed = model.transform(self.dataset)
        self.assertEquals(transformed.select("detailed_prediction.cluster").head()[0], 0, "Prediction should match")
        self.assertEquals(len(transformed.select("detailed_prediction.distances").head()[0]), 3,
                          "Size of distances array should match")

    def testUserPoints(self):
        algo = H2OKMeans(splitRatio=0.8,
                         seed=1,
                         k=3,
                         featuresCols=["sepal_len", "sepal_wid", "petal_len", "petal_wid"],
                         userPoints=[[4.9, 3.0, 1.4, 0.2], [5.6, 2.5, 3.9, 1.1], [6.5, 3.0, 5.2, 2.0]])

        model = algo.fit(self.dataset)
        self.assertEquals(model.transform(self.dataset).select("prediction").head()[0], 0, "Prediction should match")


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OKMeansTestSuite], file_name="py_unit_tests_kmeans_report")
