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

from py4j.java_gateway import JavaObject
from pyspark.ml.param import TypeConverters
from pyspark.ml.util import _jvm


class H2OTypeConverters(object):

    @staticmethod
    def toEnumListString(enumClass, nullEnabled=False):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
            module = package.__getattr__("MODULE$")
            if nullEnabled:
                converter = H2OTypeConverters.toNullableListString()
            else:
                converter = H2OTypeConverters.toListString()

            javaArray = module.getValidatedEnumValues(enumClass, converter(value), nullEnabled)

            if javaArray is None:
                return None
            else:
                return list(javaArray)

        return convert

    @staticmethod
    def toEnumString(enumClass):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
            module = package.__getattr__("MODULE$")
            return module.getValidatedEnumValue(enumClass, H2OTypeConverters.toString()(value))

        return convert

    @staticmethod
    def toNullableListString():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListString()(value)

        return convert

    @staticmethod
    def toListString():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                valueForConversion = value
                if isinstance(value, JavaObject):
                    valueForConversion = list(value)

                return TypeConverters.toListString(valueForConversion)

        return convert

    @staticmethod
    def toNullableString():
        def convert(value):
            if value is None:
                return None
            else:
                return TypeConverters.toString(value)

        return convert

    @staticmethod
    def toString():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return TypeConverters.toString(value)

        return convert

    @staticmethod
    def toNullableInt():
        def convert(value):
            if value is None:
                return None
            else:
                return TypeConverters.toInt(value)

        return convert

    @staticmethod
    def toInt():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return TypeConverters.toInt(value)

        return convert

    @staticmethod
    def toNullableFloat():
        def convert(value):
            if value is None:
                return None
            else:
                return TypeConverters.toFloat(value)

        return convert

    @staticmethod
    def toFloat():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return TypeConverters.toFloat(value)

        return convert

    @staticmethod
    def toNullableBoolean():
        def convert(value):
            if value is None:
                return None
            else:
                return TypeConverters.toBoolean(value)

        return convert

    @staticmethod
    def toBoolean():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return TypeConverters.toBoolean(value)

        return convert

    @staticmethod
    def toNullableListFloat():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListFloat()(value)

        return convert

    @staticmethod
    def toListFloat():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                valueForConversion = value
                if isinstance(value, JavaObject):
                    valueForConversion = list(value)

                return TypeConverters.toListFloat(valueForConversion)

        return convert

    @staticmethod
    def toNullableListInt():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListInt()(value)

        return convert

    @staticmethod
    def toListInt():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                valueForConversion = value
                if isinstance(value, JavaObject):
                    valueForConversion = list(value)

                return TypeConverters.toListInt(valueForConversion)

        return convert

    @staticmethod
    def toNullableListListFloat():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListListFloat()(value)

        return convert

    @staticmethod
    def toListListFloat():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return [H2OTypeConverters.toListFloat()(v) for v in TypeConverters.toList(value)]

        return convert
