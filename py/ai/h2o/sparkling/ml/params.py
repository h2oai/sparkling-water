from h2o.utils.typechecks import assert_is_type
from pyspark.ml.param import *

from py_sparkling.ml.util import validateEnumValue, \
    getDoubleArrayArrayFromIntArrayArray
from pysparkling.ml.params import H2OAlgoUnsupervisedParams


class H2OKMeansParams(H2OAlgoUnsupervisedParams):
    maxIterations = Param(Params._dummy(), "maxIterations", "Maximum number of KMeans Iterations")
    standardize = Param(Params._dummy(), "standardize", "Standardize columns before computing distances")
    init = Param(Params._dummy(), "init", "Initialization mode")
    userPoints = Param(Params._dummy(), "userPoints", "This option allows" +
                       " you to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
                       " points must have the same number of columns as the training observations. The number of rows must equal" +
                       " the number of clusters.")
    estimateK = Param(Params._dummy(), "estimateK", "If enabled, iteratively find up to k clusters.")
    k = Param(Params._dummy(), "k", "Number of clusters to generate.")

    #
    # Getters
    #
    def getMaxIterations(self):
        return self.getOrDefault(self.maxIterations)

    def getStandardize(self):
        return self.getOrDefault(self.standardize)

    def getInit(self):
        return self.getOrDefault(self.init)

    def getUserPoints(self):
        return self.getOrDefault(self.userPoints)

    def getEstimateK(self):
        return self.getOrDefault(self.estimateK)

    def getK(self):
        return self.getOrDefault(self.k)

    #
    # Setters
    #
    def setMaxIterations(self, value):
        assert_is_type(value, int)
        return self._set(maxIterations=value)

    def setStandardize(self, value):
        assert_is_type(value, bool)
        return self._set(standardize=value)

    def setInit(self, value):
        validateEnumValue(self.__getInitEnum(), value)
        return self._set(init=value)

    def setUserPoints(self, value):
        assert_is_type(value, [[int, float]])
        return self._set(userPoints=getDoubleArrayArrayFromIntArrayArray(value))

    def setEstimateK(self, value):
        assert_is_type(value, bool)
        return self._set(estimateK=value)

    def setK(self, value):
        assert_is_type(value, int)
        return self._set(k=value)

    def __getInitEnum(self):
        return "hex.kmeans$Initialization"

