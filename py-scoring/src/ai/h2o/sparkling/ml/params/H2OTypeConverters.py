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
from py4j.java_collections import JavaArray
from pyspark.ml.linalg import DenseVector, DenseMatrix
from pyspark.ml.param import TypeConverters
from pyspark.ml.util import _jvm
from pyspark.sql import DataFrame, SparkSession


class H2OTypeConverters(object):

    @staticmethod
    def toNullableListEnumString(enumClass):
        return H2OTypeConverters.toListEnumString(enumClass, True)

    @staticmethod
    def toListEnumString(enumClass, nullEnabled=False):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "EnumParamValidator$")
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
    def toNullableEnumString(enumClass):
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toEnumString(enumClass)(value)

        return convert

    @staticmethod
    def toEnumString(enumClass):
        def convert(value):
            package = getattr(_jvm().ai.h2o.sparkling.ml.params, "EnumParamValidator$")
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
    def toList(value):
        if value is None:
            raise TypeError("None is not allowed.")
        else:
            valueForConversion = value
            if isinstance(value, JavaObject):
                valueForConversion = list(value)

            return TypeConverters.toList(valueForConversion)

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
    def toNullableListBoolean():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListBoolean()(value)

        return convert

    @staticmethod
    def toListBoolean():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                valueForConversion = value
                if isinstance(value, JavaObject):
                    valueForConversion = list(value)

                if TypeConverters._can_convert_to_list(valueForConversion):
                    valueForConversion = TypeConverters.toList(valueForConversion)
                    if all(map(lambda v: type(v) == bool, valueForConversion)):
                        return [bool(v) for v in valueForConversion]
                raise TypeError("Could not convert %s to list of booleans" % valueForConversion)

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
    def toPairString():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, tuple) and len(value) == 2:
                return (H2OTypeConverters.toString()(value[0])), H2OTypeConverters.toString()(value[1])
            else:
                raise TypeError("Could not convert %s to pair of strings." % value)

        return convert

    @staticmethod
    def toNullableListPairString():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListPairString()(value)

        return convert

    @staticmethod
    def toListPairString():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return [H2OTypeConverters.toPairString()(v) for v in TypeConverters.toList(value)]

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
                return [H2OTypeConverters.toListFloat()(v) for v in H2OTypeConverters.toList(value)]

        return convert

    @staticmethod
    def toNullableListListString():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toListListString()(value)

        return convert

    @staticmethod
    def toListListString():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            else:
                return [H2OTypeConverters.toListString()(v) for v in H2OTypeConverters.toList(value)]

        return convert

    @staticmethod
    def toDenseMatrix():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, JavaObject):
                return value
            elif isinstance(value, DenseMatrix):
                package = getattr(_jvm().ai.h2o.sparkling.ml.params, "ConversionUtils$")
                module = package.__getattr__("MODULE$")
                return _jvm().org.apache.spark.ml.linalg.DenseMatrix(
                    value.numRows,
                    value.numCols,
                    module.toDoubleArray(H2OTypeConverters.toListFloat()(value.values)),
                    value.isTransposed)
            else:
                raise TypeError("Invalid type. The expected type is pyspark.ml.linalg.DenseMatrix.")

        return convert

    @staticmethod
    def toNullableListDenseMatrix():
        def convert(value):
            if value is None:
                return None
            else:
                return [H2OTypeConverters.toDenseMatrix()(v) for v in TypeConverters.toList(value)]

        return convert

    @staticmethod
    def toDenseVector():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, JavaObject):
                return value
            elif isinstance(value, DenseVector):
                package = getattr(_jvm().ai.h2o.sparkling.ml.params, "ConversionUtils$")
                module = package.__getattr__("MODULE$")
                return _jvm().org.apache.spark.ml.linalg.DenseVector(
                    module.toDoubleArray(H2OTypeConverters.toListFloat()(value.values)))
            else:
                raise TypeError("Invalid type. The expected type is pyspark.ml.linalg.DenseVector.")

        return convert

    @staticmethod
    def toNullableListDenseVector():
        def convert(value):
            if value is None:
                return None
            else:
                return [H2OTypeConverters.toDenseVector()(v) for v in TypeConverters.toList(value)]

        return convert

    @staticmethod
    def toDataFrame():
        def convert(value):
            if value is None:
                raise TypeError("None is not allowed.")
            elif isinstance(value, JavaObject):
                return value
            elif isinstance(value, DataFrame):
                return value._jdf
            else:
                raise TypeError("Invalid type. The expected type is pyspark.sql.DataFrame.")

        return convert

    @staticmethod
    def toNullableDataFrame():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toDataFrame()(value)

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
    def toNullableListJavaObject():
        def convert(value):
            if value is None:
                return None
            elif isinstance(value, list) or isinstance(value, JavaArray):
                return [H2OTypeConverters.toJavaObj()(obj) for obj in value]
            else:
                raise TypeError("Invalid type: " + str(type(value)))

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
    def toNullableDictionaryWithAnyElements():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toDictionaryWithAnyElements()(value)

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
    def toNullableDictionaryWithFloatElements():
        def convert(value):
            if value is None:
                return None
            else:
                return H2OTypeConverters.toDictionaryWithFloatElements()(value)

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

    @staticmethod
    def nullableScalaMapStringStringToDictStringAny(value):
        if value is None:
            return None
        else:
            return H2OTypeConverters.scalaMapStringStringToDictStringAny(value)

    @staticmethod
    def scalaArrayToPythonArray(array):
        if array is None:
            return None
        elif isinstance(array, JavaObject):
            return [v for v in array]
        else:
            raise TypeError("Invalid type.")

    @staticmethod
    def scala2DArrayToPython2DArray(array):
        if array is None:
            return None
        elif isinstance(array, JavaObject):
            return [H2OTypeConverters.scalaArrayToPythonArray(v) for v in array]
        else:
            raise TypeError("Invalid type.")
