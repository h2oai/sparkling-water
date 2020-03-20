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

from pyspark import SparkConf

from tests.generic_test_utils import *


def asert_h2o_frame(h2o_frame, rdd):
    assert h2o_frame.nrow == rdd.count(), "Number of rows should match"
    assert h2o_frame.ncol == 1, "Number of columns should equal 1"
    assert h2o_frame.names == ["value"], "Column should be name values"


def get_default_spark_conf(additional_conf=None):
    if additional_conf is None:
        additional_conf = {}
    conf = SparkConf(). \
        setAppName("pyunit-test"). \
        setMaster("local[*]"). \
        set("spark.driver.memory", "2g"). \
        set("spark.executor.memory", "2g"). \
        set("spark.ext.h2o.client.log.level", "DEBUG"). \
        set("spark.ext.h2o.repl.enabled", "false"). \
        set("spark.task.maxFailures", "1"). \
        set("spark.rpc.numRetries", "1"). \
        set("spark.deploy.maxExecutorRetries", "1"). \
        set("spark.network.timeout", "360s"). \
        set("spark.worker.timeout", "360"). \
        set("spark.ext.h2o.cloud.name", unique_cloud_name("test")). \
        set("spark.ext.h2o.external.start.mode", "auto"). \
        set("spark.ext.h2o.node.log.dir", "build/h2ologs-pyunit/workers"). \
        set("spark.ext.h2o.client.log.dir", "build/h2ologs-pyunit/client")

    for key in additional_conf:
        conf.set(key, additional_conf[key])

    if conf.get("spark.ext.h2o.backend.cluster.mode") == "external":
        conf.set("spark.ext.h2o.client.ip", local_ip())
        conf.set("spark.ext.h2o.external.cluster.size", "1")

    return conf


def assert_data_frames_are_identical(expected, produced):
    expected.cache()
    produced.cache()

    expectedCount = expected.count()
    producedCount = produced.count()

    assert expectedCount == producedCount,\
        'The expected data frame has %s rows whereas the produced data frame has %s rows.'\
        % (expectedCount, producedCount)

    expectedDistinctCount = expected.distinct().count()
    producedDistinctCount = produced.distinct().count()

    assert expectedDistinctCount == producedDistinctCount,\
        'The expected data frame has %s distinct rows whereas the produced data frame has %s distinct rows.' \
        % (expectedDistinctCount, producedDistinctCount)

    numberOfExtraRowsInExpected = expected.subtract(produced).count()
    numberOfExtraRowsInProduced = produced.subtract(expected).count()

    assert numberOfExtraRowsInExpected == 0 and numberOfExtraRowsInProduced == 0,\
        """The expected data frame contains %s distinct rows that are not in the produced data frame.
        The produced data frame contains %s distinct rows that are not in the expected data frame."""\
        % (numberOfExtraRowsInExpected, numberOfExtraRowsInProduced)


def assert_h2o_frames_are_identical(expected, produced):
    assert expected.get_frame_data() == produced.get_frame_data()
    expectedTypeSet = set(expected.types.items())
    producedTypeSet = set(produced.types.items())
    assert len(expectedTypeSet) == len(expectedTypeSet.intersection(producedTypeSet))
