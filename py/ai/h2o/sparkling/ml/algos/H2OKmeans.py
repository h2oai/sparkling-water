from h2o.utils.typechecks import assert_is_type
from pyspark import keyword_only
from pyspark.ml.param import *
from pyspark.ml.param import *
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator

from ai.h2o.sparkling.ml.utils import getDoubleArrayArrayFromIntArrayArray
from py_sparkling.ml.models import H2OMOJOModel
from py_sparkling.ml.util import set_double_values, validateEnumValue
from pysparkling.ml.params import H2OAlgoUnsupervisedParams
from pysparkling.spark_specifics import get_input_kwargs


class H2OKMeansParams(H2OAlgoUnsupervisedParams):
    maxIterations = Param(Params._dummy(), "maxIterations",
                          "Maximum number of KMeans iterations to find the centroids.")
    standardize = Param(Params._dummy(), "standardize",
                        "Standardize the numeric columns to have a mean of zero and unit variance.")
    init = Param(Params._dummy(), "init", "Initialization mode for finding the initial cluster centers.")
    userPoints = Param(Params._dummy(), "userPoints", "This option enables" +
                       " to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
                       " points must have the same number of columns as the training observations. The number of rows must equal" +
                       " the number of clusters.")
    estimateK = Param(Params._dummy(), "estimateK",
                      "If enabled, the algorithm tries to identify optimal number of clusters, up to k clusters.")
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
        validated = validateEnumValue(self.__getInitEnum(), value)
        return self._set(init=validated)

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


class H2OKMeans(H2OKMeansParams, JavaEstimator, JavaMLReadable, JavaMLWritable):
    @keyword_only
    def __init__(self,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 foldCol=None,
                 weightCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=0,
                 allStringColumnsToCategorical=True,
                 columnsToCategorical=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False,
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
                 maxIterations=10,
                 standardize=True,
                 init="Furthest",
                 userPoints=None,
                 estimateK=False,
                 k=2,
                 **deprecatedArgs):
        super(H2OKMeans, self).__init__()
        self._java_obj = self._new_java_obj("ai.h2o.sparkling.ml.algos.H2OKMeans", self.uid)

        self._setDefault(
            predictionCol="prediction",
            detailedPredictionCol="detailed_prediction",
            withDetailedPredictionCol=False,
            featuresCols=[],
            foldCol=None,
            weightCol=None,
            splitRatio=1.0,
            seed=-1,
            nfolds=0,
            allStringColumnsToCategorical=True,
            columnsToCategorical=[],
            convertUnknownCategoricalLevelsToNa=False,
            convertInvalidNumbersToNa=False,
            modelId=None,
            keepCrossValidationPredictions=False,
            keepCrossValidationFoldAssignment=False,
            parallelizeCrossValidation=True,
            distribution="AUTO",
            maxIterations=10,
            standardize=True,
            init="Furthest",
            userPoints=None,
            estimateK=False,
            k=2)
        kwargs = get_input_kwargs(self)
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self,
                  predictionCol="prediction",
                  detailedPredictionCol="detailed_prediction",
                  withDetailedPredictionCol=False,
                  featuresCols=[],
                  foldCol=None,
                  weightCol=None,
                  splitRatio=1.0,
                  seed=-1,
                  nfolds=0,
                  allStringColumnsToCategorical=True,
                  columnsToCategorical=[],
                  convertUnknownCategoricalLevelsToNa=False,
                  convertInvalidNumbersToNa=False,
                  modelId=None,
                  keepCrossValidationPredictions=False,
                  keepCrossValidationFoldAssignment=False,
                  parallelizeCrossValidation=True,
                  distribution="AUTO",
                  maxIterations=10,
                  standardize=True,
                  init="Furthest",
                  userPoints=None,
                  estimateK=False,
                  k=2,
                  **deprecatedArgs):
        kwargs = get_input_kwargs(self)

        validateEnumValue(self._H2OAlgoCommonParams__getDistributionEnum(), kwargs, "distribution")
        validateEnumValue(self._H2OKMeansParams__getInitEnum(), kwargs, "init")

        # We need to convert double arguments manually to floats as if we assign integer to double, py4j thinks that
        # the whole type is actually int and we get class cast exception
        double_types = ["splitRatio"]
        set_double_values(kwargs, double_types)

        if "init" in kwargs:
            kwargs["init"] = getDoubleArrayArrayFromIntArrayArray(kwargs["init"])

        return self._set(**kwargs)

    def _create_model(self, java_model):
        return H2OMOJOModel(java_model)
