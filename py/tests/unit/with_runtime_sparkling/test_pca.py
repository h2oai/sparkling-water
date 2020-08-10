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
from pyspark.sql import Row
from pyspark.sql.types import *
from pysparkling.ml import H2OPCA

from tests import unit_test_utils
from tests.unit.with_runtime_sparkling.algo_test_utils import *
from pyspark.sql.functions import bround


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OPCA")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OPCA")


def getPreconfiguredAlgorithm():
    return H2OPCA(k=2,
                  maxIterations=500,
                  pcaMethod="GramSVD",
                  seed=42,
                  convertUnknownCategoricalLevelsToNa=True,
                  transform="standardize")


def testPipelineSerialization(birdsDataset):
    [traningDataset, testingDataset] = birdsDataset.randomSplit([0.9, 0.1], 42)
    algo = getPreconfiguredAlgorithm()

    pipeline = Pipeline(stages=[algo])
    pipeline.write().overwrite().save("file://" + os.path.abspath("build/pca_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/pca_pipeline"))
    model = loadedPipeline.fit(traningDataset)
    expected = model.transform(testingDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/pca_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/pca_pipeline_model"))
    result = loadedModel.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)


def testPCAResult(birdsDataset):
    [traningDataset, testingDataset] = birdsDataset.randomSplit([0.9, 0.1], 42)
    algo = getPreconfiguredAlgorithm()
    model = algo.fit(traningDataset)
    predictions = model.transform(testingDataset)
    expected = [Row(prediction0=0, prediction1=0),
                Row(prediction0=0, prediction1=0)]
    assert predictions.select("prediction") != expected
