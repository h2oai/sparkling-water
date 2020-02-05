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

import warnings
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.SharedBackendConf import SharedBackendConf
from ai.h2o.sparkling.InternalBackendConf import InternalBackendConf
from ai.h2o.sparkling.ExternalBackendConf import ExternalBackendConf


class H2OConf(SharedBackendConf, InternalBackendConf, ExternalBackendConf):
    def __init__(self, spark):
        try:
            spark_session = spark
            if isinstance(spark, SparkContext):
                warnings.warn("Method H2OContext.getOrCreate with argument of type SparkContext is deprecated and " +
                              "parameter of type SparkSession is preferred.")
                spark_session = SparkSession.builder.getOrCreate()

            Initializer.load_sparkling_jar()
            self._do_init(spark_session)
        except:
            raise

    def _do_init(self, spark_session):
        self._spark_session = spark_session
        self._sc = self._spark_session._sc
        jvm = self._sc._jvm
        jsc = self._sc._jsc
        # Create instance of H2OConf class
        self._jconf = jvm.org.apache.spark.h2o.H2OConf(jsc)
