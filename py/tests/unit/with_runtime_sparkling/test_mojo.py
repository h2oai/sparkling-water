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

import pytest
import tempfile
import shutil
import unit_test_utils
import os

from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.functions import array, struct
from pysparkling.ml import *
from h2o.estimators import H2OGradientBoostingEstimator
from pyspark.ml.feature import VectorAssembler
from ai.h2o.sparkling.ml.models.H2OMOJOModel import H2OMOJOModel


@pytest.fixture(scope="module")
def gbmModel(prostateDataset):
    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")
    return gbm.fit(prostateDataset)


def testDomainColumns(gbmModel):
    domainValues = gbmModel.getDomainValues()
    assert domainValues["DPROS"] is None
    assert domainValues["DCAPS"] is None
    assert domainValues["VOL"] is None
    assert domainValues["AGE"] is None
    assert domainValues["PSA"] is None
    assert domainValues["capsule"] == ["0", "1"]
    assert domainValues["RACE"] is None
    assert domainValues["ID"] is None


def testTrainingParams(gbmModel):
    params = gbmModel.getTrainingParams()
    assert params["seed"] == "42"
    assert params["distribution"] == "bernoulli"
    assert params["ntrees"] == "2"
    assert len(params) == 44


def testModelCategory(gbmModel):
    category = gbmModel.getModelCategory()
    assert category == "Binomial"


def testTrainingMetrics(gbmModel):
    metrics = gbmModel.getTrainingMetrics()
    assert metrics is not None
    assert len(metrics) is 10


def testFeatureTypes(gbmModel):
    types = gbmModel.getFeatureTypes()
    assert types["DPROS"] == "Numeric"
    assert types["GLEASON"] == "Numeric"
    assert types["DCAPS"] == "Numeric"
    assert types["VOL"] == "Numeric"
    assert types["AGE"] == "Numeric"
    assert types["PSA"] == "Numeric"
    assert types["capsule"] == "Enum"
    assert types["RACE"] == "Numeric"
    assert types["ID"] == "Numeric"
    assert len(types) == 9


def testScoringHistory(gbmModel):
    scoringHistoryDF = gbmModel.getScoringHistory()
    assert scoringHistoryDF.count() > 0
    assert len(scoringHistoryDF.columns) > 0


def testFeatureImportances(gbmModel):
    featureImportancesDF = gbmModel.getFeatureImportances()
    assert featureImportancesDF.select("Variable").collect().sort() == gbmModel.getFeaturesCols().sort()
    assert len(featureImportancesDF.columns) == 4


def testFeatureImportancesAndScoringHistoryAreSameAfterSerde(gbmModel):
    expectedScoringHistoryDF = gbmModel.getScoringHistory()
    expectedFeatureImportancesDF = gbmModel.getFeatureImportances()

    filePath = "file://" + os.path.abspath("build/scoringHistoryAndFeatureImportancesSerde")
    gbmModel.write().overwrite().save(filePath)
    loadedModel = H2OMOJOModel.load(filePath)

    loadedScoringHistoryDF = loadedModel.getScoringHistory()
    loadedFeatureImportancesDF = loadedModel.getFeatureImportances()

    unit_test_utils.assert_data_frames_are_identical(expectedScoringHistoryDF, loadedScoringHistoryDF)
    unit_test_utils.assert_data_frames_are_identical(expectedFeatureImportancesDF, loadedFeatureImportancesDF)


def getCurrentMetrics():
    metrics = gbmModel.getCurrentMetrics()
    assert metrics == gbmModel.getTrainingMetrics()


@pytest.fixture(scope="module")
def prostateDatasetWithDoubles(prostateDataset):
    return prostateDataset.select(
        prostateDataset.CAPSULE.cast("string").alias("CAPSULE"),
        prostateDataset.AGE.cast("double").alias("AGE"),
        prostateDataset.RACE.cast("double").alias("RACE"),
        prostateDataset.DPROS.cast("double").alias("DPROS"),
        prostateDataset.DCAPS.cast("double").alias("DCAPS"),
        prostateDataset.PSA,
        prostateDataset.VOL,
        prostateDataset.GLEASON.cast("double").alias("GLEASON"))


def trainAndTestH2OPythonGbm(hc, dataset):
    h2oframe = hc.asH2OFrame(dataset)
    label = "CAPSULE"
    gbm = H2OGradientBoostingEstimator(seed=42)
    gbm.train(y=label, training_frame=h2oframe)
    directoryName = tempfile.mkdtemp(prefix="")
    try:
        mojoPath = gbm.download_mojo(directoryName)
        model = H2OMOJOModel.createFromMojo("file://" + mojoPath)
        return model.transform(dataset).select(
            "prediction",
            "detailed_prediction.probabilities.0",
            "detailed_prediction.probabilities.1")
    finally:
        shutil.rmtree(directoryName)


def compareH2OPythonGbmOnTwoDatasets(hc, reference, tested):
    expected = trainAndTestH2OPythonGbm(hc, reference)
    result = trainAndTestH2OPythonGbm(hc, tested)
    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testMojoTrainedWithH2OAPISupportsArrays(hc, prostateDatasetWithDoubles):
    arrayDataset = prostateDatasetWithDoubles.select(
        prostateDatasetWithDoubles.CAPSULE,
        array(
            prostateDatasetWithDoubles.AGE,
            prostateDatasetWithDoubles.RACE,
            prostateDatasetWithDoubles.DPROS,
            prostateDatasetWithDoubles.DCAPS,
            prostateDatasetWithDoubles.PSA,
            prostateDatasetWithDoubles.VOL,
            prostateDatasetWithDoubles.GLEASON).alias("features"))
    compareH2OPythonGbmOnTwoDatasets(hc, prostateDatasetWithDoubles, arrayDataset)


def testMojoTrainedWithH2OAPISupportsVectors(hc, prostateDatasetWithDoubles):
    assembler = VectorAssembler(
        inputCols=["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
        outputCol="features")
    vectorDataset = assembler.transform(prostateDatasetWithDoubles).select("CAPSULE", "features")
    compareH2OPythonGbmOnTwoDatasets(hc, prostateDatasetWithDoubles, vectorDataset)


def testMojoTrainedWithH2OAPISupportsStructs(hc, prostateDatasetWithDoubles):
    arrayDataset = prostateDatasetWithDoubles.select(
        prostateDatasetWithDoubles.CAPSULE,
        prostateDatasetWithDoubles.AGE,
        struct(
            prostateDatasetWithDoubles.RACE,
            struct(
                prostateDatasetWithDoubles.DPROS,
                prostateDatasetWithDoubles.DCAPS,
                prostateDatasetWithDoubles.PSA).alias("b"),
            prostateDatasetWithDoubles.VOL).alias("a"),
        prostateDatasetWithDoubles.GLEASON)
    compareH2OPythonGbmOnTwoDatasets(hc, prostateDatasetWithDoubles, arrayDataset)


def testMojoModelCouldBeSavedAndLoaded(gbmModel, prostateDataset):
    path = "file://" + os.path.abspath("build/testMojoModelCouldBeSavedAndLoaded")
    gbmModel.write().overwrite().save(path)
    loadedModel = H2OMOJOModel.load(path)

    expected = gbmModel.transform(prostateDataset).drop("detailed_prediction")
    result = loadedModel.transform(prostateDataset).drop("detailed_prediction")

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testGetCrossValidationSummary():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/gbm_cv.mojo"))
    summary = mojo.getCrossValidationMetricsSummary()

    assert summary.columns == ["metric", "mean", "sd", "cv_1_valid", "cv_2_valid", "cv_3_valid"]
    assert summary.count() > 0


def testCrossValidationModelsAreAvailableAfterSavingAndLoading(prostateDataset):
    path = "file://" + os.path.abspath("build/testCrossValidationModelsAreAvialableAfterSavingAndLoading")
    nfolds = 3
    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule",
                 nfolds=nfolds, keepCrossValidationModels=True)
    model = gbm.fit(prostateDataset)
    model.write().overwrite().save(path)
    loadedModel = H2OMOJOModel.load(path)
    cvModels = loadedModel.getCrossValidationModels()

    assert len(cvModels) == nfolds

    result = loadedModel.transform(prostateDataset)
    cvResult = cvModels[0].transform(prostateDataset)

    assert cvResult.schema == result.schema
    assert cvResult.count() == result.count()
    assert 0 < cvModels[0].getTrainingMetrics()['AUC'] < 1
    assert 0 < cvModels[0].getValidationMetrics()['AUC'] < 1
    assert cvModels[0].getCrossValidationMetrics() == {}
    assert cvModels[0].getModelDetails() == model.getCrossValidationModels()[0].getModelDetails()


def testCrossValidationModelsAreNoneIfKeepCrossValidationModelsIsFalse(prostateDataset):
    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule",
                 nfolds=3, keepCrossValidationModels=False)
    model = gbm.fit(prostateDataset)

    assert model.getCrossValidationModels() is None


def testMetricObjects(prostateDataset):
    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule",
                 nfolds=3, keepCrossValidationModels=False)
    model = gbm.fit(prostateDataset)

    def compareMetricValues(metricsObject, metricsMap):
        for metric in metricsMap:
            metricValue = metricsMap[metric]
            objectValue = getattr(metricsObject, "get" + metric)()
            assert(metricValue == objectValue)
        assert metricsObject.getConfusionMatrix().count() > 0
        assert len(metricsObject.getConfusionMatrix().columns) > 0
        assert metricsObject.getGainsLiftTable().count() > 0
        assert len(metricsObject.getGainsLiftTable().columns) > 0
        assert metricsObject.getMaxCriteriaAndMetricScores().count() > 0
        assert len(metricsObject.getMaxCriteriaAndMetricScores().columns) > 0
        assert metricsObject.getThresholdsAndMetricScores().count() > 0
        assert len(metricsObject.getThresholdsAndMetricScores().columns) > 0

    compareMetricValues(model.getTrainingMetricsObject(), model.getTrainingMetrics())
    compareMetricValues(model.getCrossValidationMetricsObject(), model.getCrossValidationMetrics())
    compareMetricValues(model.getCurrentMetricsObject(), model.getCurrentMetrics())
    assert model.getValidationMetricsObject() is None
    assert model.getValidationMetrics() == {}


def testGetStartTime():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/multi_model_iris.mojo"))
    assert mojo.getStartTime() == 1631392711317


def testGetEndTime():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/multi_model_iris.mojo"))
    assert mojo.getEndTime() == 1631392711360


def testGetRunTime():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/multi_model_iris.mojo"))
    assert mojo.getRunTime() == 43


def testGetDefaultThreshold():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    assert mojo.getDefaultThreshold() == 0.40858428648438255


def testGetCrossValidationModelsScoringHistory():
    mojo = H2OMOJOModel.createFromMojo("file://" + os.path.abspath("../ml/src/test/resources/gbm_cv.mojo"))
    history = mojo.getCrossValidationModelsScoringHistory()
    assert len(history) == 3
    for history_df in history:
        assert len(history_df.columns) == 16
        assert history_df.count() == 3


def testGetCrossValidationModelsScoringHistoryWhenDataIsMissing():
    mojo = H2OMOJOModel.createFromMojo("file://" + os.path.abspath("../ml/src/test/resources/deep_learning_prostate.mojo"))
    history = mojo.getCrossValidationModelsScoringHistory()
    assert len(history) == 0


def testGetModelSummary():
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_prostate.mojo"))
    summary = mojo.getModelSummary()

    assert summary.count() == 4
    assert summary.columns == ["Layer", "Units", "Type", "Dropout", "L1", "L2", "Mean Rate", "Rate RMS", "Momentum",
                               "Mean Weight", "Weight RMS", "Mean Bias", "Bias RMS"]

    collected_summary = summary.collect()
    assert collected_summary[0].asDict() == {'Layer': 1, 'Units': 8, 'Type': 'Input', 'Dropout': 0.0, 'L1': None,
                                             'L2': None, 'Mean Rate': None, 'Rate RMS': None, 'Momentum': None,
                                             'Mean Weight': None, 'Weight RMS': None, 'Mean Bias': None,
                                             'Bias RMS': None}
    assert collected_summary[1].asDict() == {'Layer': 2, 'Units': 200, 'Type': 'Rectifier', 'Dropout': 0.0, 'L1': 0.0,
                                             'L2': 0.0, 'Mean Rate': 0.006225864375919627,
                                             'Rate RMS': 0.0030197836458683014, 'Momentum': 0.0,
                                             'Mean Weight': 0.0020895117304439736, 'Weight RMS': 0.09643048048019409,
                                             'Mean Bias': 0.42625558799512825, 'Bias RMS': 0.049144044518470764}
    assert collected_summary[2].asDict() == {'Layer': 3, 'Units': 200, 'Type': 'Rectifier', 'Dropout': 0.0, 'L1': 0.0,
                                             'L2': 0.0, 'Mean Rate': 0.04241905607206281,
                                             'Rate RMS': 0.09206506609916687, 'Momentum': 0.0,
                                             'Mean Weight': -0.008243563556700311, 'Weight RMS': 0.06984925270080566,
                                             'Mean Bias': 0.9844640783479953, 'Bias RMS': 0.008990883827209473}
    assert collected_summary[3].asDict() == {'Layer': 4, 'Units': 1, 'Type': 'Linear', 'Dropout': None, 'L1': 0.0,
                                             'L2': 0.0, 'Mean Rate': 0.0006254940157668898,
                                             'Rate RMS': 0.0009573120623826981, 'Momentum': 0.0,
                                             'Mean Weight': 0.0009763148391539289, 'Weight RMS': 0.06601589918136597,
                                             'Mean Bias': 0.002604305485232783, 'Bias RMS': 1.0971281125650402e-154}
