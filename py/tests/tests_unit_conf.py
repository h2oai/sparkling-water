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
Unit tests for PySparkling H2O Configuration
"""

import unittest
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf
from pyspark.sql import SparkSession

import h2o
import unit_test_utils
import generic_test_utils


class H2OConfTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cloud_name = generic_test_utils.unique_cloud_name("h2o_conf_test")
        cls._spark = SparkSession.builder.config(
            conf=unit_test_utils.get_default_spark_conf().set("spark.ext.h2o.cloud.name",
                                                              cls._cloud_name)).getOrCreate()
        unit_test_utils.set_up_class(cls)
        h2o_conf = H2OConf(cls._spark).set_num_of_external_h2o_nodes(2)
        cls._hc = H2OContext.getOrCreate(cls._spark, h2o_conf)

    @classmethod
    def tearDownClass(cls):
        h2o.cluster().shutdown()
        unit_test_utils.tear_down_class(cls)

    # test passing h2o_conf to H2OContext
    def test_h2o_conf(self):
        self.assertEquals(self._hc.get_conf().cloud_name(), self._cloud_name,
                          "Configuration property cloud_name should match")


if __name__ == '__main__':
    generic_test_utils.run_tests([H2OConfTest], file_name="py_unit_tests_conf_report")
