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
import generic_test_utils
from integ_test_utils import *
import unittest

class YarnIntegTestSuite(unittest.TestCase):

    def test_chicago_crime(self):
            env = IntegTestEnv()

            env.set_spark_master("yarn-client")
            # Configure YARN environment
            env.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
            env.conf("spark.executor.instances", 3)
            env.conf("spark.executor.memory", "2g")
            env.conf("spark.ext.h2o.port.base", 63331)
            env.conf("spark.driver.memory", "2g")

            return_code = launch(env, "examples/scripts/ChicagoCrimeDemo.py")
            self.assertTrue(return_code == 0, "Process ended in a wrong way. It ended with return code "+str(return_code))



if __name__ == '__main__':
        generic_test_utils.run_tests([YarnIntegTestSuite], file_name="py_integ_yarn_tests_report")
