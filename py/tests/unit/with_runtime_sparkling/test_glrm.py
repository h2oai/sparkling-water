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
from pyspark.sql.types import *
from pyspark.sql.functions import bround, udf
from pysparkling.ml import H2OGLRMMOJOModel, H2OAutoEncoder, H2OGLRM, H2OGBM
from h2o.frame import H2OFrame
from tests import unit_test_utils
from pyspark.ml import Pipeline, PipelineModel

from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGLRM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGLRM")


def testUsageOfGLRMInAPipeline(prostateDataset):

    pca = H2OGLRM() \
        .setInputCols(["RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"]) \
        .setK(3)

    gbm = H2OGBM() \
        .setFeaturesCols([pca.getOutputCol()]) \
        .setLabelCol("CAPSULE")

    pipeline = Pipeline(stages=[pca, gbm])

    model = pipeline.fit(prostateDataset)
    assert model.transform(prostateDataset).groupBy("prediction").count().count() > 1


def roundPredictions(dataframe, precision):
    to_array = udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
    return dataframe.select(
        dataframe.Murder,
        dataframe.Assault,
        dataframe.UrbanPop,
        dataframe.Rape,
        bround(to_array(dataframe.output)[0], precision).alias("output0"),
        bround(to_array(dataframe.output)[1], precision).alias("output1"),
        bround(to_array(dataframe.output)[2], precision).alias("output2"),
        bround(to_array(dataframe.output)[3], precision).alias("output3"))


def getPreconfiguredAlgorithm():
    return H2OGLRM(k=4,
                   transform="Standardize",
                   loss="Quadratic",
                   gammaX=0.5,
                   gammaY=0.3,
                   seed=42,
                   outputCol="output",
                   recoverSvd=True)


def testPipelineSerialization(arrestsDataset):
    [traningDataset, testingDataset] = arrestsDataset.randomSplit([0.9, 0.1], 42)
    algo = getPreconfiguredAlgorithm()

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/glrm_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/glrm_pipeline"))
    model = loadedPipeline.fit(traningDataset)
    expected = roundPredictions(model.transform(testingDataset), 7)

    model.write().overwrite().save("file://" + os.path.abspath("build/glrm_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/glrm_pipeline_model"))
    result = roundPredictions(loadedModel.transform(testingDataset), 7)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testUserXHasEffectOnTrainedModel(spark, arrestsDataset):
    referenceAlgo = getPreconfiguredAlgorithm()
    referenceModel = referenceAlgo.fit(arrestsDataset)
    reference = roundPredictions(referenceModel.transform(arrestsDataset), 7)

    column = [5.412, 65.24, -7.54, -0.032, 2.212, 92.24, -17.54, 23.268, 0.312, 123.24, 14.46, 9.768, 1.012, 19.24,
              -15.54, -1.732, 5.412, 65.24, -7.54, -0.032, 2.212, 92.24, -17.54, 23.268, 0.312, 123.24, 14.46,
              9.76, 1.012, 19.24, -15.54, -1.732, 5.412, 65.24, -7.54, -0.032, 2.212, 92.24, -17.54, 23.268, 0.312,
              123.24, 14.46, 9.768, 1.012, 19.24, -15.54, -1.732, 5.412, 65.24]
    userXData = map(lambda value: (value, value, value, value), column)
    userXDataFrame = spark.createDataFrame(userXData, ['a', 'b', 'c', 'd'])
    algo = getPreconfiguredAlgorithm()
    algo.setUserX(userXDataFrame)
    algo.setInit("User")
    model = algo.fit(arrestsDataset)
    result = roundPredictions(model.transform(arrestsDataset), 7)

    unit_test_utils.assert_data_frames_have_different_values(reference, result)


def testUserYHasEffectOnTrainedModel(spark, arrestsDataset):
    referenceAlgo = getPreconfiguredAlgorithm()
    referenceModel = referenceAlgo.fit(arrestsDataset)
    reference = roundPredictions(referenceModel.transform(arrestsDataset), 7)

    userYData = [(5.412,  65.24,  -7.54, -0.032),
                 (2.212,  92.24, -17.54, 23.268),
                 (0.312, 123.24,  14.46,  9.768),
                 (1.012,  19.24, -15.54, -1.732)]
    userYDataFrame = spark.createDataFrame(userYData, ['a', 'b', 'c', 'd'])
    algo = getPreconfiguredAlgorithm()
    algo.setUserY(userYDataFrame)
    algo.setInit("User")
    model = algo.fit(arrestsDataset)
    result = roundPredictions(model.transform(arrestsDataset), 7)

    unit_test_utils.assert_data_frames_have_different_values(reference, result)


def testLossByColHasEffectOnTrainedModel(arrestsDataset):
    referenceAlgo = getPreconfiguredAlgorithm()
    referenceModel = referenceAlgo.fit(arrestsDataset)
    reference = roundPredictions(referenceModel.transform(arrestsDataset), 7)

    algo = getPreconfiguredAlgorithm()
    algo.setLossByCol(["absolute", "huber"])
    algo.setLossByColNames(["Murder", "Rape"])
    model = algo.fit(arrestsDataset)
    result = roundPredictions(model.transform(arrestsDataset), 7)

    unit_test_utils.assert_data_frames_have_different_values(reference, result)


def testIterationNumberHasEffectOnScoring(arrestsDataset):
    referenceAlgo = getPreconfiguredAlgorithm()
    model = referenceAlgo.fit(arrestsDataset)
    reference1 = roundPredictions(model.transform(arrestsDataset), 7)
    reference2 = roundPredictions(model.transform(arrestsDataset), 7)
    unit_test_utils.assert_data_frames_are_identical(reference1, reference2)

    model.setMaxScoringIterations(5)
    result = roundPredictions(model.transform(arrestsDataset), 7)
    unit_test_utils.assert_data_frames_have_different_values(reference1, result)


def testRepresentationFrameIsAccessible(hc, arrestsDataset):
    representationName="myFrame"
    algo = getPreconfiguredAlgorithm()
    algo.setRepresentationName(representationName)
    algo.setKeepBinaryModels(True)
    algo.fit(arrestsDataset)
    frame = H2OFrame.get_frame(representationName, full_cols=-1, light=True)
    df = hc.asSparkFrame(frame)
    assert (df.count() == arrestsDataset.count())
    assert (len(df.columns) == algo.getK())
