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
from integ_test_utils import IntegTestSuite
import test_utils

class YarnIntegTestSuite(IntegTestSuite):

    def test_chicago_crime(self):
            self.spark_master("yarn-client")
            # Configure YARN environment
            self.conf("spark.yarn.max.executor.failures", 1) # In fail of executor, fail the test
            self.conf("spark.executor.instances", 3)
            self.conf("spark.executor.memory", "2g")
            self.conf("spark.ext.h2o.port.base", 63331)
            self.conf("spark.driver.memory", "2g")

            self.launch("examples/scripts/ChicagoCrimeDemo.py")


if __name__ == '__main__':
        test_utils.run_tests(YarnIntegTestSuite, file_name="py_integ_yarn_tests_report")