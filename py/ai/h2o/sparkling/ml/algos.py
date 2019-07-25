from pyspark import keyword_only
from pyspark.ml.param import *
from pyspark.ml.util import JavaMLWritable, JavaMLReadable
from pyspark.ml.wrapper import JavaEstimator

from ai.h2o.sparkling.ml.params import H2OKMeansParams
from py_sparkling.ml.models import H2OMOJOModel
from py_sparkling.ml.util import set_double_values, validateEnumValue, \
    getDoubleArrayArrayFromIntArrayArray
from pysparkling.spark_specifics import get_input_kwargs


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
                 k=1,
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
            k=1)
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
                  k=1,
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
