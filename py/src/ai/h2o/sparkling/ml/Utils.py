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
from pyspark.sql import SparkSession


class Utils(object):
    javaDoubleMaxValue = (2 - 2 ** (-52)) * (2 ** 1023)

    @staticmethod
    def propagateValueFromDeprecatedProperty(kwargs, deprecatedOption, replacingOption):
        if deprecatedOption in kwargs:
            warnings.warn(
                "The parameter '{}' is deprecated and its usage will override a value specified via '{}'!".format(
                    deprecatedOption, replacingOption))
            kwargs[replacingOption] = kwargs[deprecatedOption]
            del kwargs[deprecatedOption]

    @staticmethod
    def getInputKwargs(instance):
        spark_version = SparkSession.builder.getOrCreate().version

        if spark_version == "2.1.0":
            return instance.__init__._input_kwargs
        else:
            # on newer versions we need to use the following variant
            return instance._input_kwargs

    @staticmethod
    def methodDeprecationWarning(old, new=None):
        Utils.__deprecationWarning(old, "method", new)

    @staticmethod
    def fieldDeprecationWarning(kwargs, old, new=None):
        if old in kwargs:
            Utils.__deprecationWarning(old, "field", new)

    @staticmethod
    def __deprecationWarning(old, type, new=None):
        if new is None:
            warnings.warn("The {} '{}' is deprecated without replacement!".format(type, old, new))
        else:
            warnings.warn("The {} '{}' is deprecated. Use '{}' instead!".format(type, old, new))
