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
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.mllib.linalg import *
from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OGBM, H2OWord2Vec

from tests import unit_test_utils


def testPipelineSerialization(craiglistDataset):
    [traningDataset, testingDataset] = craiglistDataset.randomSplit([0.9, 0.1], 42)

    tokenizer = RegexTokenizer(inputCol="jobtitle", minTokenLength=2, outputCol="tokenized")
    stopWordsRemover = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="stopWordsRemoved")
    w2v = H2OWord2Vec(sentSampleRate=0, epochs=10, inputCol=stopWordsRemover.getOutputCol(), outputCol="w2v")
    gbm = H2OGBM(labelCol="category", featuresCols=[w2v.getOutputCol()])

    pipeline = Pipeline(stages=[tokenizer, stopWordsRemover, w2v, gbm])

    pipeline.write().overwrite().save("file://" + os.path.abspath("build/w2v_pipeline"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/w2v_pipeline"))
    model = loadedPipeline.fit(traningDataset)
    expected = model.transform(testingDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/w2v_pipeline_model"))
    loadedModel = PipelineModel.load("file://" + os.path.abspath("build/w2v_pipeline_model"))
    result = loadedModel.transform(testingDataset)

    unit_test_utils.assert_data_frames_are_identical(expected, result)
