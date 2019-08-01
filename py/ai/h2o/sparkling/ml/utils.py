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
from pyspark.ml.util import _jvm


def getDoubleArrayArrayFromIntArrayArray(array):
    if array is None:
        return None
    else:
        return [getDoubleArrayFromIntArray(arr) for arr in array]


def getValidatedEnumValue(enumClass, name):
    package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
    return package.__getattr__("MODULE$").getValidatedEnumValue(enumClass, name)


def getValidatedEnumValues(enumClass, names, nullEnabled=False):
    package = getattr(_jvm().ai.h2o.sparkling.ml.params, "H2OAlgoParamsHelper$")
    return package.__getattr__("MODULE$").getValidatedEnumValues(enumClass, names, nullEnabled)


def validateEnumValue(enumClass, kwargs, name):
    if name in kwargs:
        kwargs[name] = getValidatedEnumValue(enumClass, kwargs[name])


def validateEnumValues(enumClass, kwargs, name, nullEnabled=False):
    if name in kwargs:
        kwargs[name] = getValidatedEnumValues(enumClass, kwargs[name], nullEnabled=nullEnabled)


def getDoubleArrayFromIntArray(array):
    if array is None:
        return None
    else:
        return list(map(float, array))


def arrayToDoubleArray(param, kwargs):
    if param in kwargs and kwargs[param] is not None:
        kwargs[param] = getDoubleArrayFromIntArray(kwargs[param])


def set_double_values(kwargs, values):
    for v in values:
        if v in kwargs:
            kwargs[v] = float(kwargs[v])

def propagate_value_from_deprecated_property(kwargs, from_deprecated, to_replacing):
    if from_deprecated in kwargs:
        warnings.warn("The parameter '{}' is deprecated and its usage will override a value specified via '{}'!".format(from_deprecated, to_replacing))
        kwargs[to_replacing] = kwargs[from_deprecated]
        del kwargs[from_deprecated]
