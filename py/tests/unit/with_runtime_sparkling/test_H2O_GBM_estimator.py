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
from h2o.estimators.gbm import H2OGradientBoostingEstimator
from pyspark.mllib.linalg import *
from pyspark.sql.functions import log, col
from pyspark.sql.types import *
from pysparkling.ml import H2OMOJOModel

from tests import unit_test_utils


def testLoadAndTrainMojo(hc, spark, prostateDataset):
    referenceMojo = H2OMOJOModel.createFromMojo("file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    frame = hc.asH2OFrame(prostateDataset)
    frame["CAPSULE"] = frame["CAPSULE"].asfactor()
    gbm = H2OGradientBoostingEstimator(distribution="bernoulli", ntrees=2, seed=42)
    gbm.train(y="CAPSULE", training_frame=frame)
    mojoFile = gbm.download_mojo(path=os.path.abspath("build/"), get_genmodel_jar=False)
    trainedMojo = H2OMOJOModel.createFromMojo("file://" + mojoFile)

    expect = referenceMojo.transform(prostateDataset)
    result = trainedMojo.transform(prostateDataset)

    unit_test_utils.assert_data_frames_are_identical(expect, result)


@pytest.fixture(scope="module")
def insuranceFrame(hc, spark, insuranceDatasetPath):
    df = spark \
        .read.csv( insuranceDatasetPath, header=True, inferSchema=True) \
        .withColumn("Offset", log(col("Holders")))
    frame = hc.asH2OFrame(df)
    frame["Group"] = frame["Group"].asfactor()
    frame["Age"] = frame["Age"].asfactor()
    return frame


def createGBMEstimator():
    return H2OGradientBoostingEstimator(distribution="tweedie",
                                        ntrees=600,
                                        max_depth=1,
                                        min_rows=1,
                                        learn_rate=0.1,
                                        min_split_improvement=0)


def testGBMModelWithOffsetReturnsExpectResults(insuranceFrame):
    gbm = createGBMEstimator()
    gbm.train(x=["District", "Group", "Age"], y="Claims", training_frame=insuranceFrame, offset_column="Offset")

    predictions = gbm.predict(insuranceFrame)

    assert abs(-1.869702 - gbm._model_json['output']['init_f']) < 1e-5, "expected init_f to be {0}, but got {1}". \
        format(-1.869702, gbm._model_json['output']['init_f'])
    assert abs(49.21591 - predictions.mean()[0]) < 1e-3, "expected prediction mean to be {0}, but got {1}". \
        format(49.21591, predictions.mean()[0])
    assert abs(1.0255 - predictions.min()) < 1e-4, "expected prediction min to be {0}, but got {1}". \
        format(1.0255, predictions.min())
    assert abs(392.4945 - predictions.max()) < 1e-2, "expected prediction max to be {0}, but got {1}". \
        format(392.4945, predictions.max())


def testGBMModelWithoutOffsetReturnsDifferentResults(insuranceFrame):
    gbm = createGBMEstimator()
    gbm.train(x=["District", "Group", "Age"], y="Claims", training_frame=insuranceFrame)

    predictions = gbm.predict(insuranceFrame)

    assert abs(-1.869702 - gbm._model_json['output']['init_f']) > 1e-5, "expected init_f to be different from -1.869702"
    assert abs(49.21591 - predictions.mean()[0]) > 1e-3, "expected prediction mean to be different from 49.21591"
    assert abs(1.0255 - predictions.min()) > 1e-4, "expected prediction min to be different from 1.0255"
    assert abs(392.4945 - predictions.max()) > 1e-2, "expected prediction max to be different from 392.4945"
