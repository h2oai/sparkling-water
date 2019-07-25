#
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

"""
Unit tests for PySparkling H2O Configuration
"""
import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest
from pysparkling.context import H2OContext
from pysparkling.conf import H2OConf
from pyspark.sql import SparkSession
from pysparkling.ml import *
import unit_test_utils
import generic_test_utils


class H2OConfTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._cloud_name = generic_test_utils.unique_cloud_name("h2o_conf_test")
        cls._conf = unit_test_utils.get_default_spark_conf(cls._spark_options_from_params). \
            set("spark.ext.h2o.cloud.name", cls._cloud_name)
        cls._spark = SparkSession.builder.config(conf=cls._conf).getOrCreate()
        cls._hc = H2OContext.getOrCreate(cls._spark, H2OConf(cls._spark).set_cluster_size(1))

    # test passing h2o_conf to H2OContext
    def test_h2o_conf(self):
        self.assertEquals(self._hc.get_conf().cloud_name(), self._cloud_name,
                          "Configuration property cloud_name should match")

    def test_gbm_params(self):
        gbm = H2OGBM(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
               nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
               seed=-1, distribution="Auto", ntrees=50, maxDepth=5, minRows=10.0, nbins=20, nbinsCats=1024, minSplitImprovement=1e-5,
               histogramType="AUTO", r2Stopping=1,
               nbinsTopLevel=1<<10, buildTreeOneNode=False, scoreTreeInterval=0,
               sampleRate=1.0, sampleRatePerClass=None, colSampleRateChangePerLevel=1.0, colSampleRatePerTree=1.0,
               learnRate=0.1, learnRateAnnealing=1.0, colSampleRate=1.0, maxAbsLeafnodePred=1,
               predNoiseBandwidth=0.0, convertUnknownCategoricalLevelsToNa=False, foldCol=None,
               predictionCol="prediction")

        self.assertEquals(gbm.getModelId(), None)
        self.assertEquals(gbm.getSplitRatio(), 1.0)
        self.assertEquals(gbm.getLabelCol(), "label")
        self.assertEquals(gbm.getWeightCol(), None)
        self.assertEquals(gbm.getFeaturesCols(), [])
        self.assertEquals(gbm.getAllStringColumnsToCategorical(), True)
        self.assertEquals(gbm.getColumnsToCategorical(), [])
        self.assertEquals(gbm.getNfolds(), 0)
        self.assertEquals(gbm.getKeepCrossValidationPredictions(), False)
        self.assertEquals(gbm.getKeepCrossValidationFoldAssignment(), False)
        self.assertEquals(gbm.getParallelizeCrossValidation(), True)
        self.assertEquals(gbm.getSeed(), -1)
        self.assertEquals(gbm.getDistribution(), "AUTO")
        self.assertEquals(gbm.getNtrees(), 50)
        self.assertEquals(gbm.getMaxDepth(), 5)
        self.assertEquals(gbm.getMinRows(), 10.0)
        self.assertEquals(gbm.getNbins(), 20)
        self.assertEquals(gbm.getNbinsCats(), 1024)
        self.assertEquals(gbm.getMinSplitImprovement(), 1e-5)
        self.assertEquals(gbm.getHistogramType(), "AUTO")
        self.assertEquals(gbm.getR2Stopping(), 1)
        self.assertEquals(gbm.getNbinsTopLevel(), 1<<10)
        self.assertEquals(gbm.getBuildTreeOneNode(), False)
        self.assertEquals(gbm.getScoreTreeInterval(), 0)
        self.assertEquals(gbm.getSampleRate(), 1.0)
        self.assertEquals(gbm.getSampleRatePerClass(), None)
        self.assertEquals(gbm.getColSampleRateChangePerLevel(), 1.0)
        self.assertEquals(gbm.getColSampleRatePerTree(), 1.0)
        self.assertEquals(gbm.getLearnRate(), 0.1)
        self.assertEquals(gbm.getLearnRateAnnealing(), 1.0)
        self.assertEquals(gbm.getColSampleRate(), 1.0)
        self.assertEquals(gbm.getMaxAbsLeafnodePred(), 1)
        self.assertEquals(gbm.getPredNoiseBandwidth(), 0.0)
        self.assertEquals(gbm.getConvertUnknownCategoricalLevelsToNa(), False)
        self.assertEquals(gbm.getFoldCol(), None)
        self.assertEquals(gbm.getPredictionCol(), "prediction")

    def test_dl_params(self):
        dl = H2ODeepLearning(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                             nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                             seed=-1, distribution="AUTO", epochs=10.0, l1=0.0, l2=0.0, hidden=[200,200], reproducible=False,
                             convertUnknownCategoricalLevelsToNa=False, foldCol=None,
                             predictionCol="prediction")

        self.assertEquals(dl.getModelId(), None)
        self.assertEquals(dl.getSplitRatio(), 1.0)
        self.assertEquals(dl.getLabelCol(), "label")
        self.assertEquals(dl.getWeightCol(), None)
        self.assertEquals(dl.getFeaturesCols(), [])
        self.assertEquals(dl.getAllStringColumnsToCategorical(), True)
        self.assertEquals(dl.getColumnsToCategorical(), [])
        self.assertEquals(dl.getNfolds(), 0)
        self.assertEquals(dl.getKeepCrossValidationPredictions(), False)
        self.assertEquals(dl.getKeepCrossValidationFoldAssignment(), False)
        self.assertEquals(dl.getParallelizeCrossValidation(), True)
        self.assertEquals(dl.getSeed(), -1)
        self.assertEquals(dl.getDistribution(), "AUTO")
        self.assertEquals(dl.getEpochs(), 10.0)
        self.assertEquals(dl.getL1(), 0.0)
        self.assertEquals(dl.getL2(), 0.0)
        self.assertEquals(dl.getHidden(), [200, 200])
        self.assertEquals(dl.getReproducible(), False)
        self.assertEquals(dl.getConvertUnknownCategoricalLevelsToNa(), False)
        self.assertEquals(dl.getFoldCol(), None)
        self.assertEquals(dl.getPredictionCol(), "prediction")

    def test_automl_params(self):
        automl = H2OAutoML(featuresCols=[], labelCol="label", allStringColumnsToCategorical=True, columnsToCategorical=[], splitRatio=1.0, foldCol=None,
                           weightCol=None, ignoredCols=[], includeAlgos=None, excludeAlgos=["DRF", "DeePLeArNING"], projectName="test", maxRuntimeSecs=3600.0, stoppingRounds=3,
                           stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=5, convertUnknownCategoricalLevelsToNa=True, seed=-1,
                           sortMetric="AUTO", balanceClasses=False, classSamplingFactors=None, maxAfterBalanceSize=5.0,
                           keepCrossValidationPredictions=True, keepCrossValidationModels=True, maxModels=0,
                           predictionCol="prediction")

        self.assertEquals(automl.getFeaturesCols(), [])
        self.assertEquals(automl.getLabelCol(), "label")
        self.assertEquals(automl.getAllStringColumnsToCategorical(), True)
        self.assertEquals(automl.getColumnsToCategorical(), [])
        self.assertEquals(automl.getSplitRatio(), 1.0)
        self.assertEquals(automl.getFoldCol(), None)
        self.assertEquals(automl.getWeightCol(), None)
        self.assertEquals(automl.getIgnoredCols(), [])
        self.assertEquals(automl.getIncludeAlgos(), None)
        self.assertEquals(automl.getExcludeAlgos(), ["DRF", "DeepLearning"])
        self.assertEquals(automl.getProjectName(), "test")
        self.assertEquals(automl.getMaxRuntimeSecs(), 3600.0)
        self.assertEquals(automl.getStoppingRounds(), 3)
        self.assertEquals(automl.getStoppingTolerance(), 0.001)
        self.assertEquals(automl.getStoppingMetric(), "AUTO")
        self.assertEquals(automl.getNfolds(), 5)
        self.assertEquals(automl.getConvertUnknownCategoricalLevelsToNa(), True)
        self.assertEquals(automl.getSeed(), -1)
        self.assertEquals(automl.getSortMetric(), "AUTO")
        self.assertEquals(automl.getBalanceClasses(), False)
        self.assertEquals(automl.getClassSamplingFactors(), None)
        self.assertEquals(automl.getMaxAfterBalanceSize(), 5.0)
        self.assertEquals(automl.getKeepCrossValidationPredictions(), True)
        self.assertEquals(automl.getKeepCrossValidationModels(), True)
        self.assertEquals(automl.getMaxModels(), 0)
        self.assertEquals(automl.getPredictionCol(), "prediction")

    def test_xgboost_params(self):
        xgboost = H2OXGBoost(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                             nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                             seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False, quietMode=True,
                             ntrees=50, nEstimators=0, maxDepth=6, minRows=1.0, minChildWeight=1.0, learnRate=0.3, eta=0.3, learnRateAnnealing=1.0,
                             sampleRate=1.0, subsample=1.0, colSampleRate=1.0, colSampleByLevel=1.0, colSampleRatePerTree=1.0, colsampleBytree=1.0,
                             maxAbsLeafnodePred=0.0, maxDeltaStep=0.0, scoreTreeInterval=0, initialScoreInterval=4000, scoreInterval=4000,
                             minSplitImprovement=0.0, gamma=0.0, nthread=-1, maxBins=256, maxLeaves=0,
                             minSumHessianInLeaf=100.0, minDataInLeaf=0.0, treeMethod="auto", growPolicy="depthwise",
                             booster="gbtree", dmatrixType="auto", regLambda=0.0, regAlpha=0.0, sampleType="uniform",
                             normalizeType="tree", rateDrop=0.0, oneDrop=False, skipDrop=0.0, gpuId=0, backend="auto",
                             foldCol=None, predictionCol="prediction")


        self.assertEquals(xgboost.getModelId(), None)
        self.assertEquals(xgboost.getSplitRatio(), 1.0)
        self.assertEquals(xgboost.getLabelCol(), "label")
        self.assertEquals(xgboost.getWeightCol(), None)
        self.assertEquals(xgboost.getFeaturesCols(), [])
        self.assertEquals(xgboost.getAllStringColumnsToCategorical(), True)
        self.assertEquals(xgboost.getColumnsToCategorical(), [])
        self.assertEquals(xgboost.getNfolds(), 0)
        self.assertEquals(xgboost.getKeepCrossValidationPredictions(), False)
        self.assertEquals(xgboost.getKeepCrossValidationFoldAssignment(), False)
        self.assertEquals(xgboost.getParallelizeCrossValidation(), True)
        self.assertEquals(xgboost.getSeed(), -1)
        self.assertEquals(xgboost.getDistribution(), "AUTO")
        self.assertEquals(xgboost.getConvertUnknownCategoricalLevelsToNa(), False)
        self.assertEquals(xgboost.getQuietMode(), True)
        self.assertEquals(xgboost.getNtrees(), 50)
        self.assertEquals(xgboost.getNEstimators(), 0)
        self.assertEquals(xgboost.getMaxDepth(), 6)
        self.assertEquals(xgboost.getMinRows(), 1.0)
        self.assertEquals(xgboost.getMinChildWeight(), 1.0)
        self.assertEquals(xgboost.getLearnRate(), 0.3)
        self.assertEquals(xgboost.getEta(), 0.3)
        self.assertEquals(xgboost.getLearnRateAnnealing(), 1.0)
        self.assertEquals(xgboost.getSampleRate(), 1.0)
        self.assertEquals(xgboost.getSubsample(), 1.0)
        self.assertEquals(xgboost.getColSampleRate(), 1.0)
        self.assertEquals(xgboost.getColSampleByLevel(), 1.0)
        self.assertEquals(xgboost.getColSampleRatePerTree(), 1.0)
        self.assertEquals(xgboost.getColsampleBytree(), 1.0)
        self.assertEquals(xgboost.getMaxAbsLeafnodePred(), 0.0)
        self.assertEquals(xgboost.getMaxDeltaStep(), 0.0)
        self.assertEquals(xgboost.getScoreTreeInterval(), 0)
        self.assertEquals(xgboost.getInitialScoreInterval(), 4000)
        self.assertEquals(xgboost.getScoreInterval(), 4000)
        self.assertEquals(xgboost.getMinSplitImprovement(), 0.0)
        self.assertEquals(xgboost.getGamma(), 0.0)
        self.assertEquals(xgboost.getNthread(), -1)
        self.assertEquals(xgboost.getMaxBins(), 256)
        self.assertEquals(xgboost.getMaxLeaves(), 0)
        self.assertEquals(xgboost.getMinSumHessianInLeaf(), 100.0)
        self.assertEquals(xgboost.getMinDataInLeaf(), 0.0)
        self.assertEquals(xgboost.getTreeMethod(), "auto")
        self.assertEquals(xgboost.getGrowPolicy(), "depthwise")
        self.assertEquals(xgboost.getBooster(), "gbtree")
        self.assertEquals(xgboost.getDmatrixType(), "auto")
        self.assertEquals(xgboost.getRegLambda(), 0.0)
        self.assertEquals(xgboost.getRegAlpha(), 0.0)
        self.assertEquals(xgboost.getSampleType(), "uniform")
        self.assertEquals(xgboost.getNormalizeType(), "tree")
        self.assertEquals(xgboost.getRateDrop(), 0.0)
        self.assertEquals(xgboost.getOneDrop(), False)
        self.assertEquals(xgboost.getSkipDrop(), 0.0)
        self.assertEquals(xgboost.getGpuId(), 0)
        self.assertEquals(xgboost.getBackend(), "auto")
        self.assertEquals(xgboost.getFoldCol(), None)
        self.assertEquals(xgboost.getPredictionCol(), "prediction")

    def test_glm_params(self):
        glm = H2OGLM(modelId=None, splitRatio=1.0, labelCol="label", weightCol=None, featuresCols=[], allStringColumnsToCategorical=True, columnsToCategorical=[],
                     nfolds=0, keepCrossValidationPredictions=False, keepCrossValidationFoldAssignment=False, parallelizeCrossValidation=True,
                     seed=-1, distribution="AUTO", convertUnknownCategoricalLevelsToNa=False,
                     standardize=True, family="gaussian", link="family_default", solver="AUTO", tweedieVariancePower=0.0,
                     tweedieLinkPower=0.0, alpha=[1], lambda_=None, missingValuesHandling="MeanImputation",
                     prior=-1.0, lambdaSearch=False, nlambdas=-1, nonNegative=False, exactLambdas=False,
                     lambdaMinRatio=-1.0,maxIterations=-1, intercept=True, betaEpsilon=1e-4, objectiveEpsilon=-1.0,
                     gradientEpsilon=-1.0, objReg=-1.0, computePValues=False, removeCollinearCols=False,
                     interactions=None, interactionPairs=None, earlyStopping=True, foldCol=None,
                     predictionCol="prediction")

        self.assertEquals(glm.getModelId(), None)
        self.assertEquals(glm.getSplitRatio(), 1.0)
        self.assertEquals(glm.getLabelCol(), "label")
        self.assertEquals(glm.getWeightCol(), None)
        self.assertEquals(glm.getFeaturesCols(), [])
        self.assertEquals(glm.getAllStringColumnsToCategorical(), True)
        self.assertEquals(glm.getColumnsToCategorical(), [])
        self.assertEquals(glm.getNfolds(), 0)
        self.assertEquals(glm.getKeepCrossValidationPredictions(), False)
        self.assertEquals(glm.getKeepCrossValidationFoldAssignment(), False)
        self.assertEquals(glm.getParallelizeCrossValidation(), True)
        self.assertEquals(glm.getSeed(), -1)
        self.assertEquals(glm.getDistribution(), "AUTO")
        self.assertEquals(glm.getConvertUnknownCategoricalLevelsToNa(), False)
        self.assertEquals(glm.getStandardize(), True)
        self.assertEquals(glm.getFamily(), "gaussian")
        self.assertEquals(glm.getLink(), "family_default")
        self.assertEquals(glm.getSolver(), "AUTO")
        self.assertEquals(glm.getTweedieVariancePower(), 0.0)
        self.assertEquals(glm.getTweedieLinkPower(), 0.0)
        self.assertEquals(glm.getAlpha(), [1.0])
        self.assertEquals(glm.getLambda(), None)
        self.assertEquals(glm.getMissingValuesHandling(), "MeanImputation")
        self.assertEquals(glm.getPrior(), -1.0)
        self.assertEquals(glm.getLambdaSearch(), False)
        self.assertEquals(glm.getNlambdas(), -1)
        self.assertEquals(glm.getNonNegative(), False)
        self.assertEquals(glm.getExactLambdas(), False)
        self.assertEquals(glm.getLambdaMinRatio(), -1.0)
        self.assertEquals(glm.getMaxIterations(), -1)
        self.assertEquals(glm.getIntercept(), True)
        self.assertEquals(glm.getBetaEpsilon(), 1e-4)
        self.assertEquals(glm.getObjectiveEpsilon(), -1.0)
        self.assertEquals(glm.getGradientEpsilon(), -1.0)
        self.assertEquals(glm.getObjReg(), -1.0)
        self.assertEquals(glm.getComputePValues(), False)
        self.assertEquals(glm.getRemoveCollinearCols(), False)
        self.assertEquals(glm.getInteractions(), None)
        self.assertEquals(glm.getInteractionPairs(), None)
        self.assertEquals(glm.getEarlyStopping(), True)
        self.assertEquals(glm.getFoldCol(), None)
        self.assertEquals(glm.getPredictionCol(), "prediction")

    def test_grid_params(self):
        grid = H2OGridSearch(featuresCols=[], algo=None, splitRatio=1.0, hyperParameters={}, labelCol="label", weightCol=None, allStringColumnsToCategorical=True,
                             columnsToCategorical=[], strategy="Cartesian", maxRuntimeSecs=0.0, maxModels=0, seed=-1,
                             stoppingRounds=0, stoppingTolerance=0.001, stoppingMetric="AUTO", nfolds=0, selectBestModelBy="AUTO",
                             selectBestModelDecreasing=True, foldCol=None, convertUnknownCategoricalLevelsToNa=True,
                             predictionCol="prediction")

        self.assertEquals(grid.getFeaturesCols(), [])
        self.assertEquals(grid.getSplitRatio(), 1.0)
        self.assertEquals(grid.getHyperParameters(), {})
        self.assertEquals(grid.getLabelCol(), "label")
        self.assertEquals(grid.getWeightCol(), None)
        self.assertEquals(grid.getAllStringColumnsToCategorical(), True)
        self.assertEquals(grid.getColumnsToCategorical(), [])
        self.assertEquals(grid.getStrategy(), "Cartesian")
        self.assertEquals(grid.getMaxRuntimeSecs(), 0.0)
        self.assertEquals(grid.getMaxModels(), 0)
        self.assertEquals(grid.getSeed(), -1)
        self.assertEquals(grid.getStoppingRounds(), 0)
        self.assertEquals(grid.getStoppingTolerance(), 0.001)
        self.assertEquals(grid.getStoppingMetric(), "AUTO")
        self.assertEquals(grid.getNfolds(), 0)
        self.assertEquals(grid.getSelectBestModelBy(), "AUTO")
        self.assertEquals(grid.getSelectBestModelDecreasing(), True)
        self.assertEquals(grid.getFoldCol(), None)
        self.assertEquals(grid.getConvertUnknownCategoricalLevelsToNa(), True)
        self.assertEquals(grid.getPredictionCol(), "prediction")

if __name__ == '__main__':
    generic_test_utils.run_tests([H2OConfTest], file_name="py_unit_tests_conf_report")
