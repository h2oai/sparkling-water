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
Unit tests for PySparkling Data Conversions;
"""

import unittest

import generic_test_utils
import h2o
import unit_test_utils
from pyspark.sql import SparkSession

from pysparkling.conf import H2OConf
from pysparkling.context import H2OContext


# Hadoop Smoke Test Suite
class HadoopSmokeTestSuite(unittest.TestCase):

        @classmethod
        def setUpClass(cls):
                cls._spark = SparkSession.builder.config(conf=unit_test_utils.get_default_spark_conf()).getOrCreate()
                unit_test_utils.set_up_class(cls)
                cls._hc = H2OContext.getOrCreate(cls._spark, H2OConf(cls._spark).set_num_of_external_h2o_nodes(1))

        @classmethod
        def tearDownClass(cls):
                h2o.cluster().shutdown()
                unit_test_utils.tear_down_class(cls)

        def test_import_hdfs(self):
                pass

        def test_export_hdfs(self):
                pass

        def test_import_s3a(self):
                pass

        def test_export_s3a(self):
                pass

if __name__ == '__main__':
        generic_test_utils.run_tests([HadoopSmokeTestSuite], file_name="py_hadoop_smoke_tests_report")