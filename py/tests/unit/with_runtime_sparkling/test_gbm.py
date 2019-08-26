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

import os
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OMOJOModel, H2OGBM


def testParams():
    gbm = H2OGBM(modelId=None,
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
                 distribution="Auto",
                 ntrees=50,
                 maxDepth=5,
                 minRows=10.0,
                 nbins=20,
                 nbinsCats=1024,
                 minSplitImprovement=1e-5,
                 histogramType="AUTO",
                 r2Stopping=1,
                 nbinsTopLevel=1 << 10,
                 buildTreeOneNode=False,
                 scoreTreeInterval=0,
                 sampleRate=1.0,
                 sampleRatePerClass=None,
                 colSampleRateChangePerLevel=1.0,
                 colSampleRatePerTree=1.0,
                 learnRate=0.1,
                 learnRateAnnealing=1.0,
                 colSampleRate=1.0,
                 maxAbsLeafnodePred=1,
                 predNoiseBandwidth=0.0,
                 convertUnknownCategoricalLevelsToNa=False,
                 foldCol=None,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False)

    assert gbm.getModelId() == None
    assert gbm.getSplitRatio() == 1.0
    assert gbm.getLabelCol() == "label"
    assert gbm.getWeightCol() == None
    assert gbm.getFeaturesCols() == []
    assert gbm.getAllStringColumnsToCategorical() == True
    assert gbm.getColumnsToCategorical() == []
    assert gbm.getNfolds() == 0
    assert gbm.getKeepCrossValidationPredictions() == False
    assert gbm.getKeepCrossValidationFoldAssignment() == False
    assert gbm.getParallelizeCrossValidation() == True
    assert gbm.getSeed() == -1
    assert gbm.getDistribution() == "AUTO"
    assert gbm.getNtrees() == 50
    assert gbm.getMaxDepth() == 5
    assert gbm.getMinRows() == 10.0
    assert gbm.getNbins() == 20
    assert gbm.getNbinsCats() == 1024
    assert gbm.getMinSplitImprovement() == 1e-5
    assert gbm.getHistogramType() == "AUTO"
    assert gbm.getR2Stopping() == 1
    assert gbm.getNbinsTopLevel() == 1 << 10
    assert gbm.getBuildTreeOneNode() == False
    assert gbm.getScoreTreeInterval() == 0
    assert gbm.getSampleRate() == 1.0
    assert gbm.getSampleRatePerClass() == None
    assert gbm.getColSampleRateChangePerLevel() == 1.0
    assert gbm.getColSampleRatePerTree() == 1.0
    assert gbm.getLearnRate() == 0.1
    assert gbm.getLearnRateAnnealing() == 1.0
    assert gbm.getColSampleRate() == 1.0
    assert gbm.getMaxAbsLeafnodePred() == 1
    assert gbm.getPredNoiseBandwidth() == 0.0
    assert gbm.getConvertUnknownCategoricalLevelsToNa() == False
    assert gbm.getFoldCol() == None
    assert gbm.getPredictionCol() == "prediction"
    assert gbm.getDetailedPredictionCol() == "detailed_prediction"
    assert gbm.getWithDetailedPredictionCol() == False
    assert gbm.getConvertInvalidNumbersToNa() == False


def testLoadAndTrainMojo(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))

    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")

    model = gbm.fit(prostateDataset)

    predMojo = mojo.transform(prostateDataset).repartition(1).collect()
    predModel = model.transform(prostateDataset).repartition(1).collect()

    assert len(predMojo) == len(predModel)
    for i in range(0, len(predMojo)):
        assert predMojo[i] == predModel[i]
