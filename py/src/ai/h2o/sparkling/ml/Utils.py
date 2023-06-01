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
