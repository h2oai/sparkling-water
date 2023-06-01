"""
Unit tests for PySparkling H2OKMeans
"""
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OXGBoost

from tests.unit.with_runtime_sparkling.algo_test_utils import *
from tests import unit_test_utils


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OXGBoost")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OXGBoost")


def testFitXgboostWithoutError(prostateDataset):
    xgboost = H2OXGBoost(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")

    model = xgboost.fit(prostateDataset)
    model.transform(prostateDataset).repartition(1).collect()


def testInteractionConstraintsAffectResult(spark, prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)
    featureCols = ["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]

    def createInitialXGBoostDefinition():
        return H2OXGBoost(featuresCols=featureCols, labelCol="CAPSULE", seed=1, splitRatio=0.8)

    referenceXGBoost = createInitialXGBoostDefinition()
    referenceModel = referenceXGBoost.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    xgboost = createInitialXGBoostDefinition()
    xgboost.setInteractionConstraints([["DPROS", "DCAPS"], ["PSA", "VOL", "GLEASON"]])
    model = xgboost.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)
