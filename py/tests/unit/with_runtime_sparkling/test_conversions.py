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


import h2o
import pytest
import time
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.util import _jvm
from h2o.exceptions import H2OTypeError

from tests import generic_test_utils
from tests import unit_test_utils


def testDataframeToH2OFrame(spark, hc):
    df = spark.sparkContext.parallelize([(num, "text") for num in range(0, 100)]).toDF()
    h2o_frame = hc.asH2OFrame(df)
    assert h2o_frame.nrow == df.count(), "Number of rows should match"
    assert h2o_frame.ncol == len(df.columns), "Number of columns should match"
    assert h2o_frame.names == df.columns, "Column names should match"
    assert df.first()._2 == "text", "Value should match"


def testWideDataframeToH2OFrameWithFollowingEdit(spark, hc):
    n_col = 110
    test_data_frame = spark.createDataFrame([tuple(range(n_col))])
    h2o_frame = hc.asH2OFrame(test_data_frame)
    assert h2o_frame.dim[1] == n_col, "Number of cols should match"
    assert h2o_frame['_107'] == 107, "Content of columns should be the same"
    # h2o_frame.refresh()     # this helps to pass the test
    # in commit f50dd728281d11f9a2ab3cdaeb994644b892d65a
    col_102 = '_102'
    # replace a column after the column 100
    h2o_frame[col_102] = h2o_frame[col_102].asfactor()
    h2o_frame.refresh()
    assert h2o_frame.dim[1] == n_col, "Number of cols after replace should match"


def testIntegerRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize([num for num in range(0, 100)])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 0
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testBooleanRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize([True, False, True, True, False])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 1
    assert h2o_frame[1, 0] == 0
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testStringRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize(["a", "b", "c"])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == "a"
    assert h2o_frame[2, 0] == "c"
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testFloatRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 0.5
    assert pytest.approx(h2o_frame[1, 0]) == 1.3333333333
    assert h2o_frame[2, 0] == 178
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testDoubleRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize([0.5, 1.3333333333, 178])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 0.5
    assert pytest.approx(h2o_frame[1, 0]) == 1.3333333333
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testComplexRDDToH2OFrame(spark, hc):
    rdd = spark.sparkContext.parallelize([("a", 1, 0.5), ("b", 2, 1.5)])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == "a"
    assert h2o_frame[1, 0] == "b"
    assert h2o_frame[1, 2] == 1.5
    assert h2o_frame.nrow == rdd.count(), "Number of rows should match"
    assert h2o_frame.ncol == 3, "Number of columns should match"
    assert h2o_frame.names == ["_1", "_2", "_3"], "Column names should match"


def testLongRDDToH2OFrame(spark, hc):
    min = _jvm().Integer.MIN_VALUE - 1
    max = _jvm().Integer.MAX_VALUE + 1
    rdd = spark.sparkContext.parallelize([1, min, max])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == 1
    assert h2o_frame[1, 0] == min
    assert h2o_frame[2, 0] == max
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)


def testNumericRDDtoH2OFrameWithValueTooBig(spark, hc):
    min = _jvm().Long.MIN_VALUE - 1
    max = _jvm().Long.MAX_VALUE + 1
    rdd = spark.sparkContext.parallelize([1, min, max])
    h2o_frame = hc.asH2OFrame(rdd)
    assert h2o_frame[0, 0] == str(1)
    assert h2o_frame[1, 0] == str(min)
    assert h2o_frame[2, 0] == str(max)
    unit_test_utils.asert_h2o_frame(h2o_frame, rdd)

# test transformation from h2o frame to data frame, when given h2o frame was created without calling asH2OFrame
# on h2o context
def testH2OFrameToDataframeNew(hc):
    h2o_frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
    df = hc.asSparkFrame(h2o_frame)
    assert df.count() == h2o_frame.nrow, "Number of rows should match"
    assert len(df.columns) == h2o_frame.ncol, "Number of columns should match"
    assert df.columns == h2o_frame.names, "Column names should match"


def testH2OFrameToDataframeWithSecondConversion(hc):
    h2o_frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
    df1 = hc.asSparkFrame(h2o_frame)
    df2 = hc.asSparkFrame(h2o_frame)
    assert df1.count() == df2.count(), "Number of rows should match"
    assert len(df1.columns) == len(df2.columns), "Number of columns should match"
    assert df1.columns == df2.columns, "Column names should match"


# test transformation from h2o frame to data frame, when given h2o frame was obtained using asH2OFrame method
# on h2o context
def testH2OFrameToDataframe(spark, hc):
    rdd = spark.sparkContext.parallelize(["a", "b", "c"])
    h2o_frame = hc.asH2OFrame(rdd)
    df = hc.asSparkFrame(h2o_frame)
    assert df.count() == h2o_frame.nrow, "Number of rows should match"
    assert len(df.columns) == h2o_frame.ncol, "Number of columns should match"
    assert df.columns == h2o_frame.names, "Column names should match"


# test for SW-321
def testInnerCbindTransform(hc):
    h2o_df1 = h2o.H2OFrame({'A': [1, 2, 3]})
    h2o_df2 = h2o.H2OFrame({'B': [4, 5, 6]})
    spark_frame = hc.asSparkFrame(h2o_df1.cbind(h2o_df2))
    count = spark_frame.count()
    assert count == 3, "Number of rows is 3"


# test for SW-430
def testLazyFrames(spark, hc):
    from pyspark.sql import Row
    data = [Row(c1=1, c2="first"), Row(c1=2, c2="second")]
    df = spark.createDataFrame(data)
    hf = hc.asH2OFrame(df)
    # Modify H2O frame - this should invalidate internal cache
    hf['c3'] = 3
    # Now try to convert modified H2O frame back to Spark data frame
    dfe = hc.asSparkFrame(hf)
    assert dfe.count() == len(data), "Number of rows should match"
    assert len(dfe.columns) == 3, "Number of columns should match"
    assert dfe.collect() == [Row(c1=1, c2='first', c3=3), Row(c1=2, c2='second', c3=3)]


def testSparseDataConversion(spark, hc):
    data = [(float(x), SparseVector(5000, {x: float(x)})) for x in range(1, 90)]
    df = spark.sparkContext.parallelize(data).toDF()
    t0 = time.time()
    hc.asH2OFrame(df)
    total = time.time() - t0
    assert (total < 20) == True  # The conversion should not take longer then 20 seconds


def testUnknownTypeConversion(hc):
    with pytest.raises(H2OTypeError):
        hc.asH2OFrame("unknown type")


def testConvertEmptyDataframeEmptySchema(spark, hc):
    schema = StructType([])
    empty = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    fr = hc.asH2OFrame(empty)
    assert fr.nrows == 0
    assert fr.ncols == 0


def testConvertEmptyDataframeNonEmptySchema(spark, hc):
    schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
    empty = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    fr = hc.asH2OFrame(empty)
    assert fr.nrows == 0
    assert fr.ncols == 2


def testConvertEmptyRDD(spark, hc):
    schema = StructType([])
    empty = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    fr = hc.asH2OFrame(empty)
    assert fr.nrows == 0
    assert fr.ncols == 0


@pytest.mark.parametrize(
    "timeZone,sparkType",
    [
        pytest.param(
            "UTC",
            "date",
            id="date-UTC"
        ),
        pytest.param(
            "Europe/Prague",
            "date",
            id="date-Prague"
        ),
        pytest.param(
            "America/Phoenix",
            "date",
            id="date-Phoenix"
        ),
        pytest.param(
            "UTC",
            "timestamp",
            id="timestamp-UTC"
        ),
        pytest.param(
            "Europe/Prague",
            "timestamp",
            id="timestamp-Prague"
        ),
        pytest.param(
            "America/Phoenix",
            "timestamp",
            id="timestamp-Phoenix"
        ),
    ],
)
def testConvertTimeValueFromSparkToH2OAndBack(spark, hc, timeZone, sparkType):
    spark.conf.set("spark.sql.session.timeZone", timeZone)

    expected = ["2019-04-04 00:00:00", "2020-01-01 00:00:00", "2020-02-02 00:00:00", "2020-03-03 00:00:00"]
    data = [("2019-04-04",), ("2020-01-01",), ("2020-02-02",), ("2020-03-03",)]
    df = spark.createDataFrame(data, ['strings']).select(col('strings').cast(sparkType).alias('time'))

    hf = hc.asH2OFrame(df)
    hfResultString = hf.__unicode__()
    hfParsedItems = hfResultString.split('\n')[2:6]
    hfParsedItems.sort()

    assert hfParsedItems == expected

    dfResult = hc.asSparkFrame(hf)
    dfResultRows = dfResult.select(col("time").cast("string").alias("strings")).collect()
    dfResultItems = list(map(lambda row: row[0], dfResultRows))
    dfResultItems.sort()

    assert dfResultItems == expected
