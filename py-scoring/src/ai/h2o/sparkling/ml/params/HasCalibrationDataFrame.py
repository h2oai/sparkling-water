from ai.h2o.sparkling.ml.params.H2OTypeConverters import H2OTypeConverters
from pyspark.ml.param import *


class HasCalibrationDataFrame(Params):
    calibrationDataFrame = Param(
        Params._dummy(),
        "calibrationDataFrame",
        "Calibration data frame for Platt Scaling. "
        "To enable usage of the data frame, set the parameter calibrateModel to True.",
        H2OTypeConverters.toNullableDataFrame())

    def getCalibrationDataFrame(self):
        return self.getOrDefault(self.calibrationDataFrame)

    def setCalibrationDataFrame(self, value):
        return self._set(calibrationDataFrame=value)
