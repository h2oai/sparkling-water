from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class H2OBaseMOJOParams(Params):
    convertUnknownCategoricalLevelsToNa = Param(
        Params._dummy(),
        "convertUnknownCategoricalLevelsToNa",
        "If set to 'true', the model converts unknown categorical levels to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    convertInvalidNumbersToNa = Param(
        Params._dummy(),
        "convertInvalidNumbersToNa",
        "If set to 'true', the model converts invalid numbers to NA during making predictions.",
        H2OTypeConverters.toBoolean())

    dataFrameSerializer = Param(
        Params._dummy(),
        "dataFrameSerializer",
        "A full name of a serializer used for serialization and deserialization of Spark DataFrames " +
        "to a JSON value within NullableDataFrameParam.",
        H2OTypeConverters.toString())

    ##
    # Getters
    ##
    def getConvertUnknownCategoricalLevelsToNa(self):
        return self.getOrDefault(self.convertUnknownCategoricalLevelsToNa)

    def getConvertInvalidNumbersToNa(self):
        return self.getOrDefault(self.convertInvalidNumbersToNa)

    def getDataFrameSerializer(self):
        return self.getOrDefault(self.dataFrameSerializer)

    def setDataFrameSerializer(self, value):
        return self._set(dataFrameSerializer=value)
