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
import pandas as pd

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

        def test_import_orc_hdfs(self):
                userdata_orc_hdfs = "hdfs://127.0.0.1/user/jenkins/prostate_NA.orc"
                userdata_orc_hdfs_df = h2o.import_file(path=userdata_orc_hdfs, header=1)
                assert userdata_orc_hdfs_df.ncol == 9
                assert userdata_orc_hdfs_df.nrow == 380
                assert userdata_orc_hdfs_df[0, 2] == 65.0
                assert userdata_orc_hdfs_df[4, 4] == 1.0
                assert userdata_orc_hdfs_df[379, 8] == 6.0

        def test_export_orc_hdfs(self):
                userdata_orc_hdfs = "hdfs://127.0.0.1/user/jenkins/prostate_NA.orc"
                userdata_orc_hdfs_df = h2o.import_file(path=userdata_orc_hdfs, header=1)
                userdata_orc_hdfs_export = "hdfs://127.0.0.1/user/jenkins/prostate_NA_export.orc"
                failure = False
                try:
                        h2o.export_file(frame=userdata_orc_hdfs_df, path=userdata_orc_hdfs_export, force=True)
                except:
                        failure = True
                assert not failure

        def test_import_parquet_hdfs(self):
                userdata_parquet_hdfs = "hdfs://172.17.0.24/user/jenkins/airlines-simple.snappy.parquet"
                userdata_parquet_hdfs_df = h2o.import_file(path=userdata_parquet_hdfs, header=1)
                assert userdata_parquet_hdfs_df.ncol == 13
                assert userdata_parquet_hdfs_df.nrow == 1000
                assert userdata_parquet_hdfs_df[0, 2] == "Donald"
                assert userdata_parquet_hdfs_df[4, 4] == "hmiller4@fema.gov"
                assert userdata_parquet_hdfs_df[999, 2] == "Alice"
                assert userdata_parquet_hdfs_df[999, 7] == "5602227843485236"

        def test_export_parquet_hdfs(self):
                userdata_parquet_hdfs = "hdfs://127.0.0.1/user/jenkins/airlines-simple.snappy.parquet"
                userdata_parquet_hdfs_df = h2o.import_file(path=userdata_parquet_hdfs, header=1)
                userdata_parquet_hdfs_export = "hdfs://127.0.0.1/user/jenkins/airlines-simple.snappy_export.parquet"
                failure = False
                try:
                        h2o.export_file(frame=userdata_parquet_hdfs_df,path=userdata_parquet_hdfs_export, force=True)
                except:
                        failure = True
                assert not failure

        # def test_import_xls_hdfs(self):
        #         salesorder_xls_hdfs = "hdfs://127.0.0.1/user/jenkins/salesorder.xls"
        #         salesorder_xls_hdfs_df = h2o.import_file(path=salesorder_xls_hdfs, header=1)
        #         assert salesorder_xls_hdfs_df.ncol == 7
        #         assert salesorder_xls_hdfs_df.nrow == 43
        #         assert salesorder_xls_hdfs_df[0, 0] == 42375
        #         assert salesorder_xls_hdfs_df[4, 2] == "Sorvino"
        #         assert salesorder_xls_hdfs_df[10, 4] == 90
        #         assert salesorder_xls_hdfs_df[42, 6] == 139.72
        #
        # def test_export_xls_hdfs(self):
        #         salesorder_xls_hdfs = "hdfs://127.0.0.1/user/jenkins/salesorder.xls"
        #         salesorder_xls_hdfs_df = h2o.import_file(path=salesorder_xls_hdfs, header=1)
        #         salesorder_xls_hdfs_export = "hdfs://172.17.0.24/user/h2o/salesorder_export.xls"
        #         isOK = True
        #         try:
        #                 h2o.export_file(frame=salesorder_xls_hdfs_df,path=salesorder_xls_hdfs_export,force=True)
        #         except:
        #                 isOK = False
        #         assert isOK
        #
        # def test_import_txt_hive(self):
        #         hivesample_txt_hive = "hdfs://172.17.0.24/hive/warehouse/hivesampletable/"
        #         hivesample_txt_hive_df = h2o.import_file(path=hivesample_txt_hive)
        #         assert hivesample_txt_hive_df.ncol == 11
        #         assert hivesample_txt_hive_df.nrow == 119586
        #         assert hivesample_txt_hive_df[0, 0] == 8
        #         assert hivesample_txt_hive_df[4, 4] == "Motorola"
        #         assert hivesample_txt_hive_df[5868, 9] == 0
        #         assert hivesample_txt_hive_df[119585, 10] == 1
        #
        # def test_export_txt_hive(self):
        #         hivesample_txt_hive = "hdfs://172.17.0.24/hive/warehouse/hivesampletable/"
        #         hivesample_txt_hive_df = h2o.import_file(path=hivesample_txt_hive)
        #         hivesample_txt_hive_export = "hdfs://172.17.0.24/hive/warehouse/hivesampletable_export/HiveSampleData_export.txt"
        #         isOK = True
        #         try:
        #                 h2o.export_file(frame=hivesample_txt_hive_df,path=hivesample_txt_hive_export,force=True)
        #         except:
        #                 isOK = False
        #         assert isOK
        #
        # def test_import_csv_s3_h2o(self):
        #         airlines_csv_s3 = "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv"
        #         airlines_csv_s3_df = h2o.import_file(path=airlines_csv_s3)
        #         assert airlines_csv_s3_df.ncol == 31
        #         assert airlines_csv_s3_df.nrow == 43978
        #         assert airlines_csv_s3_df[0, 0] == 1987.0
        #         assert airlines_csv_s3_df[10, 10] == "NA"
        #         assert airlines_csv_s3_df[43977, 30] == "YES"
        #
        # def test_export_csv_s3_h2o(self):
        #         airlines_csv_s3 = "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv"
        #         airlines_csv_s3_df = h2o.import_file(path=airlines_csv_s3)
        #         airlines_export = "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k_export.csv"
        #         isOK = True
        #         try:
        #                 h2o.export_file(frame=airlines_csv_s3_df, path=airlines_export, force=True)
        #         except:
        #                 isOK = False
        #         assert isOK
        #
        # def test_csv_h2o_to_sw(self):
        #         hc = self._hc
        #         airlines_csv_s3 = "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv"
        #         airlines_csv_s3_df = h2o.import_file(path=airlines_csv_s3)
        #         airlines_csv_sw_df = hc.as_spark_frame(airlines_csv_s3_df)
        #         assert airlines_csv_sw_df.head()[0] == 1987
        #         assert airlines_csv_sw_df.head()[3] == 3
        #         assertEquals(airlines_csv_s3_df.nrow, airlines_csv_sw_df.count(),"Number of rows should match")
        #         assertEquals(airlines_csv_s3_df.ncol, len(airlines_csv_sw_df.columns),"Number of columns should match")
        #         assertEquals(airlines_csv_s3_df.names, airlines_csv_sw_df.columns,"Column names should match")
        #
        # def test_csv_sw_to_h2o(self):
        #         hc = self._hc
        #         airlines_csv_s3 = "https://s3.amazonaws.com/h2o-airlines-unpacked/allyears2k.csv"
        #         airlines_csv_s3_df = h2o.import_file(path=airlines_csv_s3)
        #         airlines_csv_sw_df = hc.as_spark_frame(airlines_csv_s3_df)
        #         airlines_csv_h2o_df = hc.as_h2o_frame(airlines_csv_sw_df)
        #         assert airlines_csv_h2o_df[0,0] == 1987
        #         assert airlines_csv_h2o_df[1,21] == 0
        #         assertEquals(airlines_csv_h2o_df.nrow, airlines_csv_sw_df.count(),"Number of rows should match")
        #         assertEquals(airlines_csv_h2o_df.ncol, len(airlines_csv_sw_df.columns),"Number of columns should match")
        #         assertEquals(airlines_csv_h2o_df.names, airlines_csv_sw_df.columns,"Column names should match")
        #
        # def test_parquet_h2o_to_sw(self):
        #         hc = self._hc
        #         userdata_parquet_h2o = "hdfs://127.0.0.1/user/jenkins/userdata2.parquet"
        #         userdata_parquet_h2o_df = h2o.import_file(path=userdata_parquet_h2o,header=1)
        #         userdata_parquet_sw_df = hc.as_spark_frame(userdata_parquet_h2o_df)
        #         assert userdata_parquet_sw_df.head()[1] == 1
        #         assert userdata_parquet_sw_df.head()[3] == "Lewis"
        #         assertEquals(userdata_parquet_h2o_df.nrow, userdata_parquet_sw_df.count(),"Number of rows should match")
        #         assertEquals(userdata_parquet_h2o_df.ncol, len(userdata_parquet_sw_df.columns),"Number of columns should match")
        #         assertEquals(userdata_parquet_h2o_df.names, userdata_parquet_sw_df.columns,"Column names should match")
        #
        # def test_parquet_sw_to_h2o(self):
        #         hc = self._hc
        #         userdata_parquet_h2o = "hdfs://127.0.0.1/user/jenkins/userdata2.parquet"
        #         userdata_parquet_h2o_df = h2o.import_file(path=userdata_parquet_h2o,header=1)
        #         userdata_parquet_sw_df = hc.as_spark_frame(userdata_parquet_h2o_df)
        #         userdata_parquet_2_h2o_df = hc.as_h2o_frame(userdata_parquet_sw_df)
        #         assert userdata_parquet_2_h2o_df[0, 1] == 1
        #         assert userdata_parquet_2_h2o_df[0, 3] == "Lewis"
        #         assertEquals(userdata_parquet_2_h2o_df.nrow, userdata_parquet_sw_df.count(),"Number of rows should match")
        #         assertEquals(userdata_parquet_2_h2o_df.ncol, len(userdata_parquet_sw_df.columns),"Number of columns should match")
        #         assertEquals(userdata_parquet_2_h2o_df.names, userdata_parquet_sw_df.columns,"Column names should match")
        #
        # def test_orc_h2o_to_sw(self):
        #         hc = self._hc
        #         userdata_orc_h2o = "hdfs://127.0.0.1/user/jenkins/userdata1_orc"
        #         userdata_orc_h2o_df = h2o.import_file(path=userdata_orc_h2o,header=1)
        #         userdata_orc_sw_df = hc.as_spark_frame(userdata_orc_h2o_df)
        #         assert userdata_orc_sw_df.head()[2] == "Amanda"
        #         assert userdata_orc_sw_df.head()[5] == "Female"
        #         assertEquals(userdata_orc_h2o_df.nrow, userdata_orc_sw_df.count(),"Number of rows should match")
        #         assertEquals(userdata_orc_h2o_df.ncol, len(userdata_orc_sw_df.columns),"Number of columns should match")
        #         assertEquals(userdata_orc_h2o_df.names, userdata_orc_sw_df.columns,"Column names should match")
        #
        # def test_orc_sw_to_h2o(self):
        #         hc = self._hc
        #         userdata_orc_h2o = "hdfs://127.0.0.1/user/jenkins/userdata1_orc"
        #         userdata_orc_h2o_df = h2o.import_file(path=userdata_orc_h2o,header=1)
        #         userdata_orc_sw_df = hc.as_spark_frame(userdata_orc_h2o_df)
        #         userdata_orc_h2o_df = hc.as_h2o_frame(userdata_orc_sw_df)
        #         assert userdata_orc_h2o_df[0,2] == "Amanda"
        #         assert userdata_orc_h2o_df[0,5] == "Female"
        #         assertEquals(userdata_orc_h2o_df.nrow, userdata_orc_sw_df.count(),"Number of rows should match")
        #         assertEquals(userdata_orc_h2o_df.ncol, len(userdata_orc_sw_df.columns),"Number of columns should match")
        #         assertEquals(userdata_orc_h2o_df.names, userdata_orc_sw_df.columns,"Column names should match")
        #
        # """
        # def test_import_csv_wasb(self):
        #         cars_csv_wasb = "wasb://h2o@aurelienbriand.blob.core.windows.net/cars.csv"
        #         cars_csv_wasb_df = h2o.import_file(path=cars_csv_wasb)
        #         assert cars_csv_wasb_df.ncol == 12
        #         assert cars_csv_wasb_df.nrow == 32
        #         assert cars_csv_wasb_df[0,0] == "Mazda RX4"
        #         assert cars_csv_wasb_df[1,0] == "Mazda RX4 Wag"
        #         assert cars_csv_wasb_df[0,1] == 21
        #         assert cars_csv_wasb_df[0,11] == 4
        #         assert cars_csv_wasb_df[10,11] == 4
        # """
        # """
        # def test_export_csv_wasb(cls):
        #     cars_csv_wasb = "wasb://h2o@aurelienbriand.blob.core.windows.net/cars.csv"
        #     cars_csv_wasb_df = h2o.import_file(path=cars_csv_wasb)
        #     cars_csv_wasb_export = "wasb://h2o@aurelienbriand.blob.core.windows.net/cars_export.csv"
        #     isOK = True
        #     try:
        #         h2o.export_file(frame=cars_csv_wasb_df,path=cars_csv_wasb_export,force=True)
        #     except:
        #         isOK = False
        #     assert isOK
        # """
        #
        # def test_upload_csv_zip_hdfs(self):
        #         userdata_csv_zip_hdfs = "/home/jenkins/prostate.csv.zip"
        #         userdata_csv_zip_hdfs_df = h2o.upload_file(path=userdata_csv_zip_hdfs,header=1)
        #         assert userdata_csv_zip_hdfs_df.ncol == 9
        #         assert userdata_csv_zip_hdfs_df.nrow == 194560
        #         assert userdata_csv_zip_hdfs_df[0, 0] == 1
        #         assert userdata_csv_zip_hdfs_df[7, 8] == 7
        #         assert userdata_csv_zip_hdfs_df[19459, 8] == 9
        #
        # def test_import_http_csv_panda(self):
        #         eyestate_pd_csv = "http://www.stat.berkeley.edu/~ledell/data/eeg_eyestate_splits.csv"
        #         eyestate_pd_csv_df = pd.read_csv(eyestate_pd_csv)
        #         eyestate_h2o_csv = h2o.H2OFrame(eyestate_pd_csv_df)
        #         assert eyestate_h2o_csv[0, 0] == 4329.23
        #         assert eyestate_h2o_csv[54, 7] == 4620
        #         assert eyestate_h2o_csv.nrow == 14980
        #         assert eyestate_h2o_csv.ncol == 16


if __name__ == '__main__':
        generic_test_utils.run_tests([HadoopSmokeTestSuite], file_name="py_hadoop_smoke_tests_report")