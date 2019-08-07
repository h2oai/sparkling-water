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

from pyspark.ml.param import TypeConverters
from pyspark.ml.util import _jvm


class H2OTypeConverters(object):

    @staticmethod
    def toEnumListString(enumClass, nullEnabled=False):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
            return [e for e in
                    package.__getattr__("MODULE$").getValidatedEnumValues(enumClass, TypeConverters.toListString(value),
                                                                          nullEnabled)]

        return convert

    @staticmethod
    def toEnumString(enumClass):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
            return package.__getattr__("MODULE$").getValidatedEnumValue(enumClass, TypeConverters.toString(value))

        return convert
