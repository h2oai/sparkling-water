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
from pysparkling.ml import H2OAutoML


def testParams():
    automl = H2OAutoML(featuresCols=[],
                       labelCol="label",
                       allStringColumnsToCategorical=True,
                       columnsToCategorical=[],
                       splitRatio=1.0,
                       foldCol=None,
                       weightCol=None,
                       ignoredCols=[],
                       includeAlgos=["XGbooST"],
                       excludeAlgos=["DRF", "DeePLeArNING"],
                       projectName="test",
                       maxRuntimeSecs=3600.0,
                       stoppingRounds=3,
                       stoppingTolerance=0.001,
                       stoppingMetric="AUTO",
                       nfolds=5,
                       convertUnknownCategoricalLevelsToNa=True,
                       seed=-1,
                       sortMetric="AUTO",
                       balanceClasses=False,
                       classSamplingFactors=None,
                       maxAfterBalanceSize=5.0,
                       keepCrossValidationPredictions=True,
                       keepCrossValidationModels=True,
                       maxModels=0,
                       predictionCol="prediction",
                       detailedPredictionCol="detailed_prediction",
                       withDetailedPredictionCol=False,
                       convertInvalidNumbersToNa=False)

    assert automl.getFeaturesCols() == []
    assert automl.getLabelCol() == "label"
    assert automl.getAllStringColumnsToCategorical() == True
    assert automl.getColumnsToCategorical() == []
    assert automl.getSplitRatio() == 1.0
    assert automl.getFoldCol() == None
    assert automl.getWeightCol() == None
    assert automl.getIgnoredCols() == []
    assert automl.getIncludeAlgos() == ["XGBoost"]
    assert automl.getExcludeAlgos() == ["DRF", "DeepLearning"]
    assert automl.getProjectName() == "test"
    assert automl.getMaxRuntimeSecs() == 3600.0
    assert automl.getStoppingRounds() == 3
    assert automl.getStoppingTolerance() == 0.001
    assert automl.getStoppingMetric() == "AUTO"
    assert automl.getNfolds() == 5
    assert automl.getConvertUnknownCategoricalLevelsToNa() == True
    assert automl.getSeed() == -1
    assert automl.getSortMetric() == "AUTO"
    assert automl.getBalanceClasses() == False
    assert automl.getClassSamplingFactors() == None
    assert automl.getMaxAfterBalanceSize() == 5.0
    assert automl.getKeepCrossValidationPredictions() == True
    assert automl.getKeepCrossValidationModels() == True
    assert automl.getMaxModels() == 0
    assert automl.getPredictionCol() == "prediction"
    assert automl.getDetailedPredictionCol() == "detailed_prediction"
    assert automl.getWithDetailedPredictionCol() == False
    assert automl.getConvertInvalidNumbersToNa() == False
