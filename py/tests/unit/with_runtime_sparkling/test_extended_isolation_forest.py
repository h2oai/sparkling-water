import os
from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pysparkling.ml import H2OExtendedIsolationForest
from tests import unit_test_utils

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OExtendedIsolationForest")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OExtendedIsolationForest")


def testPipelineSerialization(prostateDataset):
    predictors = ["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]
    algo = H2OExtendedIsolationForest(featuresCols=predictors,
                                      sampleSize=256,
                                      ntrees=100,
                                      seed=1234,
                                      extensionLevel=len(predictors) - 1)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/extended_isolation_forest_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/extended_isolation_forest_pipeline"))
    model = loadedPipeline.fit(prostateDataset)
    expected = model.transform(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/extended_isolation_forest_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/extended_isolation_forest_pipeline_model"))
    result = loadedModel.transform(prostateDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testExtendedIsolationForestModelGiveDifferentPredictionsOnDifferentRecords(prostateDataset):
    [trainingDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 42)
    algo = H2OExtendedIsolationForest(seed=1)
    model = algo.fit(trainingDataset)

    result = model.transform(testingDataset)
    predictions = result.select("prediction").take(2)

    assert (predictions[0][0] != predictions[1][0])
