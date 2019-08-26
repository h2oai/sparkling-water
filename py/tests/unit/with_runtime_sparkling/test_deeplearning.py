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

import os
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OMOJOModel, H2ODeepLearning


def testParams():
    dl = H2ODeepLearning(modelId=None,
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
                         epochs=10.0,
                         l1=0.0,
                         l2=0.0,
                         hidden=[200, 200],
                         reproducible=False,
                         convertUnknownCategoricalLevelsToNa=False,
                         foldCol=None,
                         predictionCol="prediction",
                         detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)

    assert dl.getModelId() == None
    assert dl.getSplitRatio() == 1.0
    assert dl.getLabelCol() == "label"
    assert dl.getWeightCol() == None
    assert dl.getFeaturesCols() == []
    assert dl.getAllStringColumnsToCategorical() == True
    assert dl.getColumnsToCategorical() == []
    assert dl.getNfolds() == 0
    assert dl.getKeepCrossValidationPredictions() == False
    assert dl.getKeepCrossValidationFoldAssignment() == False
    assert dl.getParallelizeCrossValidation() == True
    assert dl.getSeed() == -1
    assert dl.getDistribution() == "AUTO"
    assert dl.getEpochs() == 10.0
    assert dl.getL1() == 0.0
    assert dl.getL2() == 0.0
    assert dl.getHidden() == [200, 200]
    assert dl.getReproducible() == False
    assert dl.getConvertUnknownCategoricalLevelsToNa() == False
    assert dl.getFoldCol() == None
    assert dl.getPredictionCol() == "prediction"
    assert dl.getDetailedPredictionCol() == "detailed_prediction"
    assert dl.getWithDetailedPredictionCol() == False
    assert dl.getConvertInvalidNumbersToNa() == False


def testLoadAndTrainMojo(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_prostate.mojo"))

    dl = H2ODeepLearning(seed=42, reproducible=True, labelCol="CAPSULE")

    model = dl.fit(prostateDataset)

    predMojo = mojo.transform(prostateDataset).repartition(1).collect()
    predModel = model.transform(prostateDataset).repartition(1).collect()

    assert len(predMojo) == len(predModel)
    for i in range(0, len(predMojo)):
        assert predMojo[i] == predModel[i]
