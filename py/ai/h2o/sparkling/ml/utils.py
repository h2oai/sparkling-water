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
