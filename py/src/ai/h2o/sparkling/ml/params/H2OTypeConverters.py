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

    @staticmethod
    def toJavaObj():
        def convert(value):
            if value is None:
                return None
            elif isinstance(value, JavaObject):
                return value
            elif isinstance(value._java_obj, JavaObject):
                value._transfer_params_to_java()
                return value._java_obj
            else:
                raise TypeError("Invalid type.")

        return convert

    @staticmethod
    def toH2OGridSearchSupportedAlgo():
        def convert(value):
            javaObj = H2OTypeConverters.toJavaObj()(value)
            if javaObj is None:
                return None
            else:
                package = getattr(_jvm().ai.h2o.sparkling.ml.algos, "H2OGridSearch$SupportedAlgos$")
                module = package.__getattr__("MODULE$")
                module.checkIfSupported(javaObj)
                return javaObj

        return convert

    @staticmethod
    def toDictionaryWithAnyElements():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, JavaObject):
                keys = [k for k in value.keySet().toArray()]
                map = {}
                for k in keys:
                    map[k] = [v for v in value.get(k)]
                return map
            elif isinstance(value, dict):
                return value
            else:
                raise TypeError("Invalid type.")

        return convert

    @staticmethod
    def toDictionaryWithFloatElements():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, JavaObject):
                return {k: TypeConverters.toFloat(value[k]) for k in value.keySet().toArray()}
            elif isinstance(value, dict):
                return {k: TypeConverters.toFloat(v) for k, v in value.items()}
            else:
                raise TypeError("Invalid type.")

        return convert


    @staticmethod
    def scalaMapStringDictStringToStringDictString(value):
        if value is None:
            raise TypeError("None is not allowed.")
        elif isinstance(value, JavaObject):
            it = value.toIterator()
            map = {}
            while it.hasNext():
                pair = it.next()
                arr = pair._2()
                if arr is None:
                    map[pair._1()] = None
                else:
                    map[pair._1()] = [v for v in arr]
            return map
        else:
            raise TypeError("Invalid type.")

    @staticmethod
    def scalaMapStringStringToDictStringAny(value):
        if value is None:
            raise TypeError("None is not allowed.")
        elif isinstance(value, JavaObject):
            it = value.toIterator()
            map = {}
            while it.hasNext():
                pair = it.next()
                map[pair._1()] = pair._2()
            return map
        else:
            raise TypeError("Invalid type.")
