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

from tests.unit.with_runtime_sparkling.algo_test_utils import *
from tests import unit_test_utils


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OXGBoost")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OXGBoost")


def testFitXgboostWithoutError(prostateDataset):
    xgboost = H2OXGBoost(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")

    model = xgboost.fit(prostateDataset)
    model.transform(prostateDataset).repartition(1).collect()


def testInteractionConstraintsAffectResult(spark, prostateDataset):
    [traningDataset, testingDataset] = prostateDataset.randomSplit([0.9, 0.1], 1)
    featureCols = ["AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]

    def createInitialXGBoostDefinition():
        return H2OXGBoost(featuresCols=featureCols, labelCol="CAPSULE", seed=1, splitRatio=0.8)

    referenceXGBoost = createInitialXGBoostDefinition()
    referenceModel = referenceXGBoost.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    xgboost = createInitialXGBoostDefinition()
    xgboost.setInteractionConstraints([["DPROS", "DCAPS"], ["PSA", "VOL", "GLEASON"]])
    model = xgboost.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)
