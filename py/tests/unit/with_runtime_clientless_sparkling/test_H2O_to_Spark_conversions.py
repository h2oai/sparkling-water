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
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.context import H2OContext

from tests import generic_test_utils
from tests import unit_test_utils
from tests.unit.with_runtime_clientless_sparkling.clientless_test_utils import *


@pytest.fixture(scope="module")
def hc(spark):
    conf = createH2OConf(spark)
    return H2OContext.getOrCreate(spark, conf)


def testH2OFrameToDataframe(hc):
    frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
    df = hc.as_spark_frame(frame)
    assert df.count() == frame.nrow, "Number of rows should match"
    assert len(df.columns) == frame.ncol, "Number of columns should match"
    assert df.columns == frame.names, "Column names should match"


def testH2OFrameToDataframeWithSecondConversion(hc):
    frame = h2o.upload_file(generic_test_utils.locate("smalldata/prostate/prostate.csv"))
    df1 = hc.as_spark_frame(frame)
    df2 = hc.as_spark_frame(frame)
    assert df1.count() == df2.count(), "Number of rows should match"
    assert len(df1.columns) == len(df2.columns), "Number of columns should match"
    assert df1.columns == df2.columns, "Column names should match"


@pytest.mark.parametrize(
    "data,sparkType",
    [
        pytest.param(
            [1, 2, 3, 4, -3, -2, -1],
            ByteType(),
            id="byte"
        ),
        pytest.param(
            [1001, 1002, 1003, 1004, -1003, -1002, -1001],
            ShortType(),
            id="short"
        ),
        pytest.param(
            [100001, 100002, 100003, 100004, -100003, -100002, -100001],
            IntegerType(),
            id="integer"
        ),
        pytest.param(
            [100000000001, 100000000002, 100000000003, 100000000004, -100000000003, -100000000002, -100000000001],
            LongType(),
            id="long"
        ),
        pytest.param(
            [1.1, 2.2, 3.3, 4.4, -3.3, -2.2, -1.1],
            DoubleType(),
            id="double"
        ),
        pytest.param(
            ["a", "b", "c", "d", "c", "b", "a"],
            StringType(),
            id="string"
        ),
    ],
)
def testH2OFrameOfSpecificTypeToDataframe(spark, hc, data, sparkType):
    columnName = 'A'
    schema = StructType([
        StructField(columnName, sparkType, False),
    ])

    originalDF = spark.createDataFrame(map(lambda i: (i,), data), schema)
    frame = h2o.H2OFrame(data, column_names=[columnName])

    transformedDF = hc.as_spark_frame(frame)

    unit_test_utils.assert_data_frames_are_identical(originalDF, transformedDF)
    assert originalDF.dtypes == transformedDF.dtypes


# test for SW-321
def testInnerCbindTransform(hc):
    frame1 = h2o.H2OFrame({'A': [1, 2, 3]})
    frame2 = h2o.H2OFrame({'B': [4, 5, 6]})
    df = hc.as_spark_frame(frame1.cbind(frame2))
    count = df.count()
    assert count == 3, "Number of rows is 3"

# test for SW-430
def testLazyFrames(spark, hc):
    from pyspark.sql import Row
    data = [Row(c1=1, c2="first"), Row(c1=2, c2="second")]
    df = spark.createDataFrame(data)
    hf = hc.as_h2o_frame(df)
    # Modify H2O frame - this should invalidate internal cache
    hf['c3'] = 3
    # Now try to convert modified H2O frame back to Spark data frame
    dfe = hc.as_spark_frame(hf)
    assert dfe.count() == len(data), "Number of rows should match"
    assert len(dfe.columns) == 3, "Number of columns should match"
    assert dfe.collect() == [Row(c1=1, c2='first', c3=3), Row(c1=2, c2='second', c3=3)]
