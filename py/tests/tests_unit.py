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
Unit tests for PySparkling;
"""

import unittest

from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf
from pyspark import SparkContext, SparkConf

import h2o
import test_utils

# Test of transformations from dataframe/rdd to h2o frame and from h2o frame back to dataframe
class FrameTransformationsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._sc = SparkContext(conf = test_utils.get_default_spark_conf())
        test_utils.set_up_class(cls)
        cls._hc = H2OContext.getOrCreate(cls._sc, H2OConf(cls._sc).set_num_of_external_h2o_nodes(2))

    @classmethod
    def tearDownClass(cls):
        test_utils.tear_down_class(cls)

    # test transformation from dataframe to h2o frame
    def test_df_to_h2o_frame(self):
        hc = self._hc
        df = self._sc.parallelize([(num,"text") for num in range(0,100)]).toDF()
        h2o_frame = hc.as_h2o_frame(df)
        self.assertEquals(h2o_frame.nrow, df.count(),"Number of rows should match")
        self.assertEquals(h2o_frame.ncol, len(df.columns),"Number of columns should match")
        self.assertEquals(h2o_frame.names, df.columns,"Column names should match")
        self.assertEquals(df.first()._2, "text","Value should match")

     # test transformation from RDD consisting of python integers to h2o frame
    def test_rdd_int_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize([num for num in range(0,100)])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0], 0, "Value should match")
        test_utils.asert_h2o_frame(self,h2o_frame,rdd)

    # test transformation from RDD consisting of python booleans to h2o frame
    def test_rdd_bool_to_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize([True, False, True, True, False])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0],1,"Value should match")
        self.assertEquals(h2o_frame[1,0],0,"Value should match")
        test_utils.asert_h2o_frame(self,h2o_frame,rdd)

    # test transformation from RDD consisting of python strings to h2o frame
    def test_rdd_str_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize(["a","b","c"])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0],"a","Value should match")
        self.assertEquals(h2o_frame[2,0],"c","Value should match")
        test_utils.asert_h2o_frame(self,h2o_frame,rdd)

    # test transformation from RDD consisting of python floats to h2o frame
    def test_rdd_float_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize([0.5,1.3333333333,178])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0],0.5,"Value should match")
        self.assertEquals(h2o_frame[1,0],1.3333333333,"Value should match")
        test_utils.asert_h2o_frame(self,h2o_frame,rdd)

    # test transformation from RDD consisting of python doubles to h2o frame
    def test_rdd_double_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize([0.5,1.3333333333,178])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0],0.5,"Value should match")
        self.assertEquals(h2o_frame[1,0],1.3333333333,"Value should match")
        test_utils.asert_h2o_frame(self, h2o_frame, rdd)

    # test transformation from RDD consisting of python complex types to h2o frame
    def test_rdd_complex_h2o_frame_1(self):
        hc = self._hc
        rdd = self._sc.parallelize([("a",1,0.5),("b",2,1.5)])
        h2o_frame = hc.as_h2o_frame(rdd)
        self.assertEquals(h2o_frame[0,0],"a","Value should match")
        self.assertEquals(h2o_frame[1,0],"b","Value should match")
        self.assertEquals(h2o_frame[1,2],1.5,"Value should match")
        self.assertEquals(h2o_frame.nrow, rdd.count(),"Number of rows should match")
        self.assertEquals(h2o_frame.ncol, 3,"Number of columns should match")
        self.assertEquals(h2o_frame.names, ["_1","_2","_3"],"Column names should match")

    # test transformation from RDD consisting of python long to h2o frame
    def test_rdd_long_h2o_frame(self):
        hc = self._hc
        rdd = self._sc.parallelize([1,55555555555555555555555555])
        with self.assertRaises(ValueError):
            h2o_frame = hc.as_h2o_frame(rdd)

    # test transformation from h2o frame to data frame, when given h2o frame was created without calling as_h2o_frame
    # on h2o context
    def test_h2o_frame_2_data_frame_new(self):
        hc = self._hc
        h2o_frame = h2o.upload_file("../examples/smalldata/prostate.csv")
        df = hc.as_spark_frame(h2o_frame)
        self.assertEquals(df.count(), h2o_frame.nrow, "Number of rows should match")
        self.assertEquals(len(df.columns), h2o_frame.ncol, "Number of columns should match")
        self.assertEquals(df.columns,h2o_frame.names, "Column names should match")

    # test transformation from h2o frame to data frame, when given h2o frame was obtained using as_h2o_frame method
    # on h2o context
    def test_h2o_frame_2_data_frame_2(self):
        hc = self._hc
        rdd = self._sc.parallelize(["a","b","c"])
        h2o_frame = hc.as_h2o_frame(rdd)
        df = hc.as_spark_frame(h2o_frame)
        self.assertEquals(df.count(), h2o_frame.nrow, "Number of rows should match")
        self.assertEquals(len(df.columns), h2o_frame.ncol, "Number of columns should match")
        self.assertEquals(df.columns, h2o_frame.names, "Column names should match")

    # test for SW-321
    def test_inner_cbind_transform(self):
        hc = self._hc
        import h2o
        h2o_df1 = h2o.H2OFrame({'A': [1, 2, 3]})
        h2o_df2 = h2o.H2OFrame({'B': [4, 5, 6]})
        spark_frame = hc.as_spark_frame(h2o_df1.cbind(h2o_df2))
        count = spark_frame.count()
        self.assertEquals(count, 3, "Number of rows is 3")

class H2OConfTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._sc = SparkContext(conf = test_utils.get_default_spark_conf().set("spark.ext.h2o.cloud.name", "test-cloud"))
        test_utils.set_up_class(cls)
        h2o_conf = H2OConf(cls._sc).set_num_of_external_h2o_nodes(2)
        cls._hc = H2OContext.getOrCreate(cls._sc, h2o_conf)

    @classmethod
    def tearDownClass(cls):
        test_utils.tear_down_class(cls)

    # test passing h2o_conf to H2OContext
    def test_h2o_conf(self):
        self.assertEquals(self._hc.get_conf().cloud_name(),"test-cloud", "Configuration property cloud_name should match")


if __name__ == '__main__':
    test_utils.run_tests([FrameTransformationsTest], file_name="py_unit_tests_report")
    #test_utils.run_tests([FrameTransformationsTest, H2OConfTest], file_name="py_unit_tests_report")
