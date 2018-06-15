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
        env.set_spark_master("local-cluster[3,1,2048]")
        env.conf("spark.ext.h2o.port.base", 63331)

        return_code = launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "gbm")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))

    def test_pipeline_deep_learning(self):
        env = IntegTestEnv()
        env.set_spark_master("local-cluster[3,1,2048]")
        env.conf("spark.ext.h2o.port.base", 63331)

        return_code = launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "dl")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))

    def test_pipeline_automl(self):
        env = IntegTestEnv()
        env.set_spark_master("local-cluster[3,1,2048]")
        env.conf("spark.ext.h2o.port.base", 63331)

        return_code = launch(env, "examples/pipelines/ham_or_spam_multi_algo.py", "automl")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))

    def test_import_pysparkling_standalone_app(self):
        env = IntegTestEnv()
        env.set_spark_master("local-cluster[3,1,2048]")
        env.conf("spark.ext.h2o.port.base", 63331)

        return_code = launch(env, "examples/scripts/tests/pysparkling_ml_import_overrides_spark_test.py")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))


if __name__ == '__main__':
    generic_test_utils.run_tests([LocalIntegTestSuite], file_name="py_integ_local_tests_report")
