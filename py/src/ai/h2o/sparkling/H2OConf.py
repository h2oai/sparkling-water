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
from ai.h2o.sparkling.ExternalBackendConf import ExternalBackendConf
from ai.h2o.sparkling.Initializer import Initializer
from ai.h2o.sparkling.InternalBackendConf import InternalBackendConf
from ai.h2o.sparkling.SharedBackendConf import SharedBackendConf
from pyspark.ml.util import _jvm


class H2OConf(SharedBackendConf, InternalBackendConf, ExternalBackendConf):
    def __init__(self, spark=None):
        try:
            if spark is not None:
                warnings.warn(
                    "Constructor H2OConf(spark) with spark argument is deprecated. Please use just H2OConf(). "
                    "The argument will be removed in release 3.32.")
            Initializer.load_sparkling_jar()
            self._jconf = _jvm().org.apache.spark.h2o.H2OConf()
        except:
            raise
