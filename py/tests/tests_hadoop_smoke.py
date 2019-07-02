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

import generic_test_utils
import h2o
import unit_test_utils
import unittest
import uuid
from datetime import datetime
from pandas.util.testing import assert_frame_equal
from pyspark.sql import SparkSession

from pysparkling.conf import H2OConf
from pysparkling.context import H2OContext


# Hadoop Smoke Test Suite
class HadoopSmokeTestSuite(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._spark = SparkSession.builder.config(
            conf=unit_test_utils.get_default_spark_conf().setMaster("yarn-client")).getOrCreate()
        unit_test_utils.set_up_class(cls)
        cls._hc = H2OContext.getOrCreate(cls._spark, H2OConf(cls._spark).set_cluster_size(1))

    @classmethod
    def tearDownClass(cls):
        h2o.cluster().shutdown()
        unit_test_utils.tear_down_class(cls)

    def test_import_orc_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/prostate_NA.orc", header=1)
        assert fr.ncol == 9
        assert fr.nrow == 380
        assert fr[0, 2] == 65.0
        assert fr[4, 4] == 1.0
        assert fr[379, 8] == 6.0

    def test_export_orc_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/prostate_NA.orc", header=1)
        export_path = "hdfs://127.0.0.1/user/jenkins/prostate_NA_export.orc"
        failure = False
        try:
            h2o.export_file(frame=fr, path=export_path, force=True)
        except:
            failure = True
        assert not failure

        imported = h2o.import_file(path=export_path, header=1)
        assert imported.ncol == fr.ncol
        assert imported.nrow == fr.nrow

    def test_import_parquet_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/airlines-simple.snappy.parquet", header=1)
        assert fr.ncol == 12
        assert fr.nrow == 24421
        assert fr[0, 0] == "f1987"
        assert fr[0, 11] == 1.0
        assert fr[24420, 6] == "UA"

    def test_export_parquet_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/airlines-simple.snappy.parquet", header=1)
        export_path = "hdfs://127.0.0.1/user/jenkins/airlines-simple.snappy_export.parquet"
        failure = False
        try:
            h2o.export_file(frame=fr, path=export_path, force=True)
        except:
            failure = True
        assert not failure

        imported = h2o.import_file(path=export_path, header=1)
        assert imported.ncol == fr.ncol
        assert imported.nrow == fr.nrow

    def test_import_xls_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/iris.xls", header=1)
        assert fr.ncol == 5
        assert fr.nrow == 150
        assert fr[0, 0] == 5.1
        assert fr[0, 4] == "Iris-setosa"
        assert fr[149, 4] == "Iris-virginica"

    def test_export_xls_hdfs(self):
        fr = h2o.import_file(path="hdfs://127.0.0.1/user/jenkins/iris.xls", header=1)
        export_path = "hdfs://127.0.0.1/user/jenkins/iris_export.xls"
        failure = False
        try:
            h2o.export_file(frame=fr, path=export_path, force=True)
        except:
            failure = True
        assert not failure

        imported = h2o.import_file(path=export_path, header=1)
        assert imported.ncol == fr.ncol
        assert imported.nrow == fr.nrow

    @unittest.skip("skip")
    def test_import_hive(self):
        connection_url = "jdbc:hive2://localhost:10000/default"
        select_query = "select * from airlinestest"
        username = "hive"
        password = ""
        fr = h2o.import_sql_select(connection_url, select_query, username, password)
        assert fr.ncol == 12
        assert fr.nrow == 2691
        assert fr[0, 0] == "f1987"
        assert fr[2690, 0] == "f2000"
        assert fr[2690, 11] == 1.0

    @unittest.skip("skip")
    def test_s3n_import(self):
        fr = h2o.import_file("s3n://data.h2o.ai/h2o-open-tour/2016-nyc/weather.csv")
        assert fr.ncol == 27
        assert fr.nrow == 9768

    def test_s3a_import(self):
        fr = h2o.import_file("s3a://data.h2o.ai/h2o-open-tour/2016-nyc/weather.csv")
        assert fr.ncol == 27
        assert fr.nrow == 9768

    def s3_import_export(self, scheme):
        local_frame = h2o.import_file("/home/0xdiag/smalldata/logreg/prostate.csv")
        timestamp = datetime.today().utcnow().strftime("%Y%m%d-%H%M%S")
        unique_suffix = str(uuid.uuid4())
        s3_path = scheme + "://test.0xdata.com/h2o-hadoop-tests/test-export/" + scheme + "/exported." + \
                  timestamp + "." + unique_suffix + ".csv.zip"
        h2o.export_file(local_frame, s3_path)
        s3_frame = h2o.import_file(s3_path)
        assert_frame_equal(local_frame.as_data_frame(), s3_frame.as_data_frame())

    def test_s3a_import_export(self):
        self.s3_import_export("s3a")

    @unittest.skip("skip")
    def test_s3n_import_export(self):
        self.s3_import_export("s3n")


if __name__ == '__main__':
    generic_test_utils.run_tests([HadoopSmokeTestSuite], file_name="py_hadoop_smoke_tests_report")
