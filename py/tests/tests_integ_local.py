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
import unittest
import sys
import os
from tests.integ_test_utils import *
from tests.generic_test_utils import run_tests
sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable

class LocalIntegTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = get_default_spark_conf(cls._spark_options_from_params)
        conf["spark.master"] = "local[*]"
        conf["spark.submit.pyFiles"] = sys.argv[1]
        cls._conf = conf

    def test_pipeline_gbm_mojo(self):
        return_code = launch(self._conf, "examples/pipelines/ham_or_spam_multi_algo.py", param="gbm")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))

    def test_pipeline_deep_learning(self):
        return_code = launch(self._conf, "examples/pipelines/ham_or_spam_multi_algo.py", param="dl")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))

    def test_pipeline_xgboost(self):
        return_code = launch(self._conf, "examples/pipelines/ham_or_spam_multi_algo.py", param="xgboost")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))

    def test_pipeline_automl(self):
        return_code = launch(self._conf, "examples/pipelines/ham_or_spam_multi_algo.py", param="automl")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))

    def test_import_pysparkling_standalone_app(self):
        return_code = launch(self._conf, "examples/scripts/tests/pysparkling_ml_import_overrides_spark_test.py")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))


if __name__ == '__main__':
    run_tests([LocalIntegTestSuite], file_name="py_integ_local_tests_report")
