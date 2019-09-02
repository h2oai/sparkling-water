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
from pysparkling.ml import H2OGLM


def testParams():
    glm = H2OGLM(modelId=None,
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
                 standardize=True,
                 family="gaussian",
                 link="family_default",
                 solver="AUTO",
                 tweedieVariancePower=0.0,
                 tweedieLinkPower=0.0,
                 alphaValue=[1],
                 lambdaValue=None,
                 missingValuesHandling="MeanImputation",
                 prior=-1.0,
                 lambdaSearch=False,
                 nlambdas=-1,
                 nonNegative=False,
                 exactLambdas=False,
                 lambdaMinRatio=-1.0,
                 maxIterations=-1,
                 intercept=True,
                 betaEpsilon=1e-4,
                 objectiveEpsilon=-1.0,
                 gradientEpsilon=-1.0,
                 objReg=-1.0,
                 computePValues=False,
                 removeCollinearCols=False,
                 interactions=None,
                 interactionPairs=None,
                 earlyStopping=True,
                 foldCol=None,
                 predictionCol="prediction",
                 detailedPredictionCol="detailed_prediction",
                 withDetailedPredictionCol=False,
                 convertInvalidNumbersToNa=False)

    assert glm.getModelId() == None
    assert glm.getSplitRatio() == 1.0
    assert glm.getLabelCol() == "label"
    assert glm.getWeightCol() == None
    assert glm.getFeaturesCols() == []
    assert glm.getAllStringColumnsToCategorical() == True
    assert glm.getColumnsToCategorical() == []
    assert glm.getNfolds() == 0
    assert glm.getKeepCrossValidationPredictions() == False
    assert glm.getKeepCrossValidationFoldAssignment() == False
    assert glm.getParallelizeCrossValidation() == True
    assert glm.getSeed() == -1
    assert glm.getDistribution() == "AUTO"
    assert glm.getConvertUnknownCategoricalLevelsToNa() == False
    assert glm.getStandardize() == True
    assert glm.getFamily() == "gaussian"
    assert glm.getLink() == "family_default"
    assert glm.getSolver() == "AUTO"
    assert glm.getTweedieVariancePower() == 0.0
    assert glm.getTweedieLinkPower() == 0.0
    assert glm.getAlphaValue() == [1.0]
    assert glm.getLambdaValue() == None
    assert glm.getMissingValuesHandling() == "MeanImputation"
    assert glm.getPrior() == -1.0
    assert glm.getLambdaSearch() == False
    assert glm.getNlambdas() == -1
    assert glm.getNonNegative() == False
    assert glm.getExactLambdas() == False
    assert glm.getLambdaMinRatio() == -1.0
    assert glm.getMaxIterations() == -1
    assert glm.getIntercept() == True
    assert glm.getBetaEpsilon() == 1e-4
    assert glm.getObjectiveEpsilon() == -1.0
    assert glm.getGradientEpsilon() == -1.0
    assert glm.getObjReg() == -1.0
    assert glm.getComputePValues() == False
    assert glm.getRemoveCollinearCols() == False
    assert glm.getInteractions() == None
    assert glm.getInteractionPairs() == None
    assert glm.getEarlyStopping() == True
    assert glm.getFoldCol() == None
    assert glm.getPredictionCol() == "prediction"
    assert glm.getDetailedPredictionCol() == "detailed_prediction"
    assert glm.getWithDetailedPredictionCol() == False
    assert glm.getConvertInvalidNumbersToNa() == False


def testPipelineSerialization(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8)

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/glm_pipeline"))
    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/glm_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/glm_pipeline_model"))

    loadedModel.transform(prostateDataset).count()


def testPropagationOfPredictionCol(prostateDataset):
    predictionCol = "my_prediction_col_name"
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8,
                  predictionCol=predictionCol)

    model = algo.fit(prostateDataset)
    columns = model.transform(prostateDataset).columns
    assert True == (predictionCol in columns)
