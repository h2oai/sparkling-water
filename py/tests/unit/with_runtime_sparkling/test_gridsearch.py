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

from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OGridSearch, H2OGBM, H2OXGBoost, H2ODeepLearning, H2OGLM


def testParams():
    grid = H2OGridSearch(featuresCols=[],
                         algo=None,
                         splitRatio=1.0,
                         hyperParameters={},
                         labelCol="label",
                         weightCol=None,
                         allStringColumnsToCategorical=True,
                         columnsToCategorical=[],
                         strategy="Cartesian",
                         maxRuntimeSecs=0.0,
                         maxModels=0,
                         seed=-1,
                         stoppingRounds=0,
                         stoppingTolerance=0.001,
                         stoppingMetric="AUTO",
                         nfolds=0,
                         selectBestModelBy="AUTO",
                         foldCol=None,
                         convertUnknownCategoricalLevelsToNa=True,
                         predictionCol="prediction",
                         detailedPredictionCol="detailed_prediction",
                         withDetailedPredictionCol=False,
                         convertInvalidNumbersToNa=False)

    assert grid.getFeaturesCols() == []
    assert grid.getSplitRatio() == 1.0
    assert grid.getHyperParameters() == {}
    assert grid.getLabelCol() == "label"
    assert grid.getWeightCol() == None
    assert grid.getAllStringColumnsToCategorical() == True
    assert grid.getColumnsToCategorical() == []
    assert grid.getStrategy() == "Cartesian"
    assert grid.getMaxRuntimeSecs() == 0.0
    assert grid.getMaxModels() == 0
    assert grid.getSeed() == -1
    assert grid.getStoppingRounds() == 0
    assert grid.getStoppingTolerance() == 0.001
    assert grid.getStoppingMetric() == "AUTO"
    assert grid.getNfolds() == 0
    assert grid.getSelectBestModelBy() == "AUTO"
    assert grid.getFoldCol() == None
    assert grid.getConvertUnknownCategoricalLevelsToNa() == True
    assert grid.getPredictionCol() == "prediction"
    assert grid.getDetailedPredictionCol() == "detailed_prediction"
    assert grid.getWithDetailedPredictionCol() == False
    assert grid.getConvertInvalidNumbersToNa() == False


def gridSearchTester(algo, prostateDataset):
    grid = H2OGridSearch(labelCol="AGE", hyperParameters={"_seed": [1, 2, 3]}, splitRatio=0.8, algo=algo,
                         strategy="RandomDiscrete", maxModels=3, maxRuntimeSecs=60, selectBestModelBy="RMSE")

    pipeline = Pipeline(stages=[grid])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/grid_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/grid_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/grid_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/grid_pipeline_model"))

    loadedModel.transform(prostateDataset).count()


def testPipelineSerializationGBM(prostateDataset):
    gridSearchTester(H2OGBM(), prostateDataset)


def testPipelineSerializationGLM(prostateDataset):
    gridSearchTester(H2OGLM(), prostateDataset)


def testPipelineSerializationDeepLearning(prostateDataset):
    gridSearchTester(H2ODeepLearning(), prostateDataset)


def testPipelineSerializationXGBoost(prostateDataset):
    gridSearchTester(H2OXGBoost(), prostateDataset)
