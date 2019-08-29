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
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pyspark.sql.types import *
from pysparkling.ml import H2ODRF

def testParams():
    drf = H2ODRF(binomialDoubleTrees=False,
                 mtries=-1,
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
                 modelId=None,
                 keepCrossValidationPredictions=False,
                 keepCrossValidationFoldAssignment=False,
                 parallelizeCrossValidation=True,
                 distribution="AUTO",
                 labelCol="label",
                 foldCol=None,
                 weightCol=None,
                 splitRatio=1.0,
                 seed=-1,
                 nfolds=0,
                 allStringColumnsToCategorical=True,
                 columnsToCategorical=[],
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 featuresCols=[],
                 convertUnknownCategoricalLevelsToNa=False,
                 convertInvalidNumbersToNa=False)

    assert drf.getBinomialDoubleTrees() == False
    assert drf.getMtries() == -1
    assert drf.getNtrees() == 50
    assert drf.getMaxDepth() == 5
    assert drf.getMinRows() == 10.0
    assert drf.getNbins() == 20
    assert drf.getNbinsCats() == 1024
    assert drf.getMinSplitImprovement() == 1e-5
    assert drf.getHistogramType() == "AUTO"
    assert drf.getR2Stopping() == 1
    assert drf.getNbinsTopLevel() == 1 << 10
    assert drf.getBuildTreeOneNode() == False
    assert drf.getScoreTreeInterval() == 0
    assert drf.getSampleRate() == 1.0
    assert drf.getSampleRatePerClass() == None
    assert drf.getColSampleRateChangePerLevel() == 1.0
    assert drf.getColSampleRatePerTree() == 1.0
    assert drf.getModelId() == None
    assert drf.getKeepCrossValidationPredictions() == False
    assert drf.getKeepCrossValidationFoldAssignment() == False
    assert drf.getParallelizeCrossValidation() == True
    assert drf.getDistribution() == "AUTO"
    assert drf.getLabelCol() == "label"
    assert drf.getFoldCol() == None
    assert drf.getWeightCol() == None
    assert drf.getSplitRatio() == 1.0
    assert drf.getSeed() == -1
    assert drf.getNfolds() == 0
    assert drf.getAllStringColumnsToCategorical() == True
    assert drf.getColumnsToCategorical() == []
    assert drf.getPredictionCol() == "prediction"
    assert drf.getDetailedPredictionCol() == "detailed_prediction"
    assert drf.getWithDetailedPredictionCol() == False
    assert drf.getFeaturesCols() == []
    assert drf.getConvertUnknownCategoricalLevelsToNa() == False
    assert drf.getConvertInvalidNumbersToNa() == False


def testPipelineSerialization(prostateDataset):
    algo = H2ODRF(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/drf_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/drf_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/drf_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/drf_pipeline_model"))

    loadedModel.transform(prostateDataset).count()
