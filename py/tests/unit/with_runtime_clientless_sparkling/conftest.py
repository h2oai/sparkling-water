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

import pytest
from pyspark.sql import SparkSession
from pysparkling.conf import H2OConf
from pysparkling.context import H2OContext

from tests import unit_test_utils


@pytest.fixture(scope="session")
def spark(spark_conf):
    conf = unit_test_utils.get_default_spark_conf(spark_conf)
    return SparkSession.builder.config(conf=conf).getOrCreate()


@pytest.fixture(scope="session")
def hc(spark):
    conf = H2OConf(spark)
    conf.set_cluster_size(1)
    conf.set("spark.ext.h2o.rest.api.based.client", "true")
    conf.use_auto_cluster_start()
    conf.set_external_cluster_mode()
    conf.set_h2o_node_web_enabled()
    return H2OContext.getOrCreate(spark, conf)


@pytest.fixture(scope="session")
def prostateDataset(spark):
    return spark.read.csv("file://" + unit_test_utils.locate("smalldata/prostate/prostate.csv"),
                          header=True, inferSchema=True)
