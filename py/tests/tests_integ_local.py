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
Integration tests for pySparkling for Spark running in local
"""
import generic_test_utils
from integ_test_utils import *
import unittest

class LocalIntegTestSuite(unittest.TestCase):

    def test_pipeline_gbm_mojo(self):
        env = IntegTestEnv()

        env.set_spark_master("local")
        env.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
        env.conf("spark.executor.instances", 3)
        env.conf("spark.executor.memory", "2g")
        env.conf("spark.ext.h2o.port.base", 63331)
        env.conf("spark.driver.memory", "2g")

        launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "gbm")

    def test_pipeline_deep_learning(self):
        env = IntegTestEnv()

        env.set_spark_master("local")
        # Configure YARN environment
        env.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
        env.conf("spark.executor.instances", 3)
        env.conf("spark.executor.memory", "2g")
        env.conf("spark.ext.h2o.port.base", 63331)
        env.conf("spark.driver.memory", "2g")

        launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "dl")

    def test_pipeline_automl(self):
        env = IntegTestEnv()

        env.set_spark_master("local")
        # Configure YARN environment
        env.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
        env.conf("spark.executor.instances", 3)
        env.conf("spark.executor.memory", "2g")
        env.conf("spark.ext.h2o.port.base", 63331)
        env.conf("spark.driver.memory", "2g")

        launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "automl")

    def test_import_pysparkling_standalone_app(self):
        env = IntegTestEnv()

        env.set_spark_master("local")
        # Configure YARN environment
        env.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
        env.conf("spark.executor.instances", 3)
        env.conf("spark.executor.memory", "2g")
        env.conf("spark.ext.h2o.port.base", 63331)
        env.conf("spark.driver.memory", "2g")

        launch(env, "py/scripts/tests/pysparkling_ml_import_overrides_spark_test.py")


if __name__ == '__main__':
    generic_test_utils.run_tests([LocalIntegTestSuite], file_name="py_integ_local_tests_report")
