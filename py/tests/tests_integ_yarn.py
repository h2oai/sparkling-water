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
Integration tests for pySparkling for Spark running in YARN mode
"""
import unittest
import sys
import os
sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from tests.integ_test_utils import *
from tests.generic_test_utils import run_tests


class YarnIntegTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf = get_default_spark_conf(cls._spark_options_from_params)
        conf["spark.master"] = "local[*]"
        conf["spark.submit.pyFiles"] = sys.argv[1]
        # Configure YARN environment
        conf["spark.yarn.max.executor.failures"] = "1"  # In fail of executor, fail the test
        conf["spark.executor.instances"] = "1"
        cls._conf = conf

    def test_xgboost_medium(self):
        return_code = launch(self._conf, "examples/scripts/tests/xgboost_test_medium.py")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))

    def test_chicago_crime(self):
        return_code = launch(self._conf, "examples/scripts/ChicagoCrimeDemo.py")
        self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code))


if __name__ == '__main__':
    run_tests([YarnIntegTestSuite], file_name="py_integ_yarn_tests_report")
