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
import pytest

from pyspark.ml import Pipeline, PipelineModel
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OGLM
from tests import unit_test_utils
from py4j.protocol import Py4JJavaError
from pyspark.sql.utils import AnalysisException

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGLM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGLM")


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


def testPlugValuesAffectResult(spark, carsDatasetPath):
    carsDataset=spark.read.csv(carsDatasetPath, header=True, inferSchema=True)
    carsDataset=carsDataset.withColumn("economy_20mpg", carsDataset.economy_20mpg.cast("string"))
    [traningDataset, testingDataset] = carsDataset.randomSplit([0.9, 0.1], 1)

    def createInitialGlmDefinition():
        featuresCols=["economy","displacement", "power", "weight", "acceleration", "year", "economy_20mpg"]
        return H2OGLM(featuresCols=featuresCols, labelCol="cylinders", seed=1,splitRatio=0.8)

    referenceGlm = createInitialGlmDefinition()
    referenceModel = referenceGlm.fit(traningDataset)
    referenceResult = referenceModel.transform(testingDataset)

    plugValues = {
        "economy": 1.1,
        "displacement": 2.2,
        "power": 3.3,
        "weight": 4.4,
        "acceleration": 5.5,
        "year": 2000,
        "economy_20mpg": "0"}
    glm = createInitialGlmDefinition()
    glm.setMissingValuesHandling("PlugValues")
    glm.setPlugValues(plugValues)
    model = glm.fit(traningDataset)
    result = model.transform(testingDataset)

    unit_test_utils.assert_data_frames_have_different_values(referenceResult, result)


def testInteractionColumnNamesArePassedWithoutException(spark):
    data = [(0.0, "a", 2.0),
            (float("nan"), "b", 8.0),
            (0.0, "a", 4.0),
            (1.0, "b", 1.0)]
    df = spark.createDataFrame(data, ["x", "y", "z"])

    plugValues = {"x": 0, "x_y.a": 1, "x_y.b": 2, "y": "b"}
    glm = H2OGLM(
        labelCol="z",
        seed=42,
        ignoreConstCols=False,
        standardize=False,
        family="gaussian",
        missingValuesHandling="PlugValues",
        plugValues=plugValues)

    glm.fit(df)

def createInitialGlmDefinitionForRandomCols():
    return H2OGLM(featuresCols=["x1", "x3", "x5", "x6"],
                  labelCol="y",
                  family="gaussian",
                  randomFamily=["gaussian"],
                  randomLink=["identity"],
                  HGLM=True,
                  calcLike=True)

def testRandomColsArePropagatedToInternals(semiconductorDataset):
    semiconductorDataset = semiconductorDataset.withColumn("Device", semiconductorDataset.Device.cast("string"))

    referenceDeepLearning = createInitialGlmDefinitionForRandomCols()
    with pytest.raises(Py4JJavaError, match=r".*Need to specify the random component columns for HGLM.*"):
        referenceDeepLearning.fit(semiconductorDataset)

    glm = createInitialGlmDefinitionForRandomCols()
    glm.setRandomCols(["Device"])
    glm.setIgnoredCols(["Device"])
    glm.fit(semiconductorDataset)


def testRandomColsMustBeWithinTrainingDataset(semiconductorDataset):
    glm = createInitialGlmDefinitionForRandomCols()
    glm.setRandomCols(["someColumn"])
    with pytest.raises(AnalysisException, match=r".*cannot resolve '`someColumn`' given input columns.*"):
        glm.fit(semiconductorDataset)
