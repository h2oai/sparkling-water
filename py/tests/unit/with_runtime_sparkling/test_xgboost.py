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
Unit tests for PySparkling H2OKMeans
"""
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OXGBoost


def testParams():
    xgboost = H2OXGBoost(modelId=None,
                         splitRatio=1.0,
                         labelCol="label",
                         weightCol=None,
                         featuresCols=[],
                         allStringColumnsToCategorical=True,
                         columnsToCategorical=[],
                         nfolds=0,
                         keepCrossValidationPredictions=False,
                         keepCrossValidationFoldAssignment=False,
                         parallelizeCrossValidation=True,
                         seed=-1,
                         distribution="AUTO",
                         convertUnknownCategoricalLevelsToNa=False,
                         quietMode=True,
                         ntrees=50,
                         nEstimators=0,
                         maxDepth=6,
                         minRows=1.0,
                         minChildWeight=1.0,
                         learnRate=0.3,
                         eta=0.3,
                         learnRateAnnealing=1.0,
                         sampleRate=1.0,
                         subsample=1.0,
                         colSampleRate=1.0,
                         colSampleByLevel=1.0,
                         colSampleRatePerTree=1.0,
                         colSampleByTree=1.0,
                         maxAbsLeafnodePred=0.0,
                         maxDeltaStep=0.0,
                         scoreTreeInterval=0,
                         initialScoreInterval=4000,
                         scoreInterval=4000,
                         minSplitImprovement=0.0,
                         gamma=0.0,
                         nthread=-1,
                         maxBins=256,
                         maxLeaves=0,
                         minSumHessianInLeaf=100.0,
                         minDataInLeaf=0.0,
                         treeMethod="auto",
                         growPolicy="depthwise",
                         booster="gbtree",
                         dmatrixType="auto",
                         regLambda=0.0,
                         regAlpha=0.0,
                         sampleType="uniform",
                         normalizeType="tree",
                         rateDrop=0.0,
                         oneDrop=False,
                         skipDrop=0.0,
                         gpuId=0,
                         backend="auto",
                         foldCol=None,
                         predictionCol="prediction",
                         detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)

    assert xgboost.getModelId() == None
    assert xgboost.getSplitRatio() == 1.0
    assert xgboost.getLabelCol() == "label"
    assert xgboost.getWeightCol() == None
    assert xgboost.getFeaturesCols() == []
    assert xgboost.getAllStringColumnsToCategorical() == True
    assert xgboost.getColumnsToCategorical() == []
    assert xgboost.getNfolds() == 0
    assert xgboost.getKeepCrossValidationPredictions() == False
    assert xgboost.getKeepCrossValidationFoldAssignment() == False
    assert xgboost.getParallelizeCrossValidation() == True
    assert xgboost.getSeed() == -1
    assert xgboost.getDistribution() == "AUTO"
    assert xgboost.getConvertUnknownCategoricalLevelsToNa() == False
    assert xgboost.getQuietMode() == True
    assert xgboost.getNtrees() == 50
    assert xgboost.getNEstimators() == 0
    assert xgboost.getMaxDepth() == 6
    assert xgboost.getMinRows() == 1.0
    assert xgboost.getMinChildWeight() == 1.0
    assert xgboost.getLearnRate() == 0.3
    assert xgboost.getEta() == 0.3
    assert xgboost.getLearnRateAnnealing() == 1.0
    assert xgboost.getSampleRate() == 1.0
    assert xgboost.getSubsample() == 1.0
    assert xgboost.getColSampleRate() == 1.0
    assert xgboost.getColSampleByLevel() == 1.0
    assert xgboost.getColSampleRatePerTree() == 1.0
    assert xgboost.getColSampleByTree() == 1.0
    assert xgboost.getMaxAbsLeafnodePred() == 0.0
    assert xgboost.getMaxDeltaStep() == 0.0
    assert xgboost.getScoreTreeInterval() == 0
    assert xgboost.getInitialScoreInterval() == 4000
    assert xgboost.getScoreInterval() == 4000
    assert xgboost.getMinSplitImprovement() == 0.0
    assert xgboost.getGamma() == 0.0
    assert xgboost.getNthread() == -1
    assert xgboost.getMaxBins() == 256
    assert xgboost.getMaxLeaves() == 0
    assert xgboost.getMinSumHessianInLeaf() == 100.0
    assert xgboost.getMinDataInLeaf() == 0.0
    assert xgboost.getTreeMethod() == "auto"
    assert xgboost.getGrowPolicy() == "depthwise"
    assert xgboost.getBooster() == "gbtree"
    assert xgboost.getDmatrixType() == "auto"
    assert xgboost.getRegLambda() == 0.0
    assert xgboost.getRegAlpha() == 0.0
    assert xgboost.getSampleType() == "uniform"
    assert xgboost.getNormalizeType() == "tree"
    assert xgboost.getRateDrop() == 0.0
    assert xgboost.getOneDrop() == False
    assert xgboost.getSkipDrop() == 0.0
    assert xgboost.getGpuId() == 0
    assert xgboost.getBackend() == "auto"
    assert xgboost.getFoldCol() == None
    assert xgboost.getPredictionCol() == "prediction"
    assert xgboost.getDetailedPredictionCol() == "detailed_prediction"
    assert xgboost.getWithDetailedPredictionCol() == False
    assert xgboost.getConvertInvalidNumbersToNa() == False
