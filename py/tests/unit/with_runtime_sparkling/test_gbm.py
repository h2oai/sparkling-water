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
import json

from pyspark.mllib.linalg import *
from pyspark.sql.types import *
from pysparkling.ml import H2OGBM, H2OMOJOModel, H2OSupervisedMOJOModel, H2OTreeBasedSupervisedMOJOModel
from pyspark.sql.functions import log, col, min, max, mean, lit
from h2o.estimators.gbm import H2OGradientBoostingEstimator

from tests import unit_test_utils
from tests.unit.with_runtime_sparkling.algo_test_utils import *


def testParamsPassedByConstructor():
    assertParamsViaConstructor("H2OGBM")


def testParamsPassedBySetters():
    assertParamsViaSetters("H2OGBM")


def testLoadAndTrainMojo(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))

    gbm = H2OGBM(ntrees=2, seed=42, distribution="bernoulli", labelCol="capsule")

    model = gbm.fit(prostateDataset)

    predMojo = mojo.transform(prostateDataset).repartition(1).collect()
    predModel = model.transform(prostateDataset).repartition(1).collect()

    assert len(predMojo) == len(predModel)
    for i in range(0, len(predMojo)):
        assert predMojo[i] == predModel[i]


@pytest.fixture(scope="module")
def dataset(spark):
    return spark \
        .read.csv("file://" + unit_test_utils.locate("smalldata/insurance.csv"), header=True, inferSchema=True) \
        .withColumn("Offset", log(col("Holders")))


@pytest.fixture(scope="module")
def gbmModelWithOffset(dataset):
    gbm=H2OGBM(distribution="tweedie",
               ntrees=600,
               maxDepth=1,
               minRows=1,
               learnRate=0.1,
               minSplitImprovement=0,
               featuresCols=["District","Group","Age"],
               labelCol="Claims",
               offsetCol="Offset")
    return gbm.fit(dataset)


@pytest.fixture(scope="module")
def savedGbmModel(gbmModelWithOffset):
    path = "file://" + os.path.abspath("build/gbm_model_with_offset")
    gbmModelWithOffset.write().overwrite().save(path)
    return path + "/mojo_model"

def testMOJOModelReturnsExpectedResultWhenOffsetColumnsIsSet(gbmModelWithOffset, dataset):
    predictionCol = col("prediction")
    predictionsDF = gbmModelWithOffset.transform(dataset)
    detailsDF = predictionsDF.select(min(predictionCol).alias("min"),
                                     max(predictionCol).alias("max"),
                                     mean(predictionCol).alias("mean"))
    result = detailsDF.first()
    modelDetials = json.loads(gbmModelWithOffset.getModelDetails())

    assert abs(-1.869702 - modelDetials['init_f']) < 1e-5, "Expected init_f to be {0}, but got {1}". \
        format(-1.869702, modelDetials['init_f'])
    assert abs(49.21591 - result["mean"]) < 1e-3, "Expected prediction mean to be {0}, but got {1}". \
        format(49.21591, result["mean"])
    assert abs(1.0255 - result["min"]) < 1e-4, "Expected prediction min to be {0}, but got {1}". \
        format(1.0255, result["min"])
    assert abs(392.4945 - result["max"]) < 1e-2, "Expected prediction max to be {0}, but got {1}". \
        format(392.4945, result["max"])
    assert gbmModelWithOffset.getOffsetCol() == "Offset", "Offset column must be propagated to the MOJO model."


def testMOJOModelReturnsDifferentResultWithZeroOffset(gbmModelWithOffset, dataset):
    predictionCol = col("prediction")
    predictionsDF = gbmModelWithOffset.transform(dataset.withColumn("Offset",lit(0.0)))
    detailsDF = predictionsDF.select(min(predictionCol).alias("min"),
                                     max(predictionCol).alias("max"),
                                     mean(predictionCol).alias("mean"))
    result = detailsDF.first()

    assert abs(49.21591 - result["mean"]) > 1e-3, "Mean with zero offset must be different from 49.21591."
    assert abs(1.0255 - result["min"]) > 1e-4, "Minimum with zero offset must be different from 1.0255."
    assert abs(392.4945 - result["max"]) > 1e-2, "Maximum with zero offset must be different from 392.4945."
    assert gbmModelWithOffset.getOffsetCol() == "Offset", "Offset column must be propagated to the MOJO model."


def testMOJOModelReturnsSameResultAsBinaryModelWhenOffsetColumnsIsSet(hc, dataset):
    [trainingDataset, testingDataset] =  dataset.randomSplit([0.8, 0.2], 1)
    trainingFrame = hc.asH2OFrame(trainingDataset)
    testingFrame = hc.asH2OFrame(testingDataset)
    gbm = H2OGradientBoostingEstimator(distribution="tweedie",
                                       ntrees=600,
                                       max_depth=1,
                                       min_rows=1,
                                       learn_rate=0.1,
                                       min_split_improvement=0)
    gbm.train(x=["District","Group","Age"], y="Claims", training_frame=trainingFrame, offset_column="Offset")

    mojoFile = gbm.download_mojo(path=os.path.abspath("build/"), get_genmodel_jar=False)
    mojoModel = H2OMOJOModel.createFromMojo("file://" + mojoFile)

    binaryModelResult = hc.asSparkFrame(gbm.predict(testingFrame))
    mojoResult = mojoModel.transform(testingDataset).select("prediction")

    unit_test_utils.assert_data_frames_are_identical(binaryModelResult, mojoResult)
    assert mojoModel.getOffsetCol() == "Offset", "Offset column must be propagated to the MOJO model."


def testMonotoneConstraintsGetProperlyPropagatedToJavaBackend():
    gbm = H2OGBM(monotoneConstraints={"District": -1, "Group": 1})

    gbm._transfer_params_to_java()
    constraints = gbm._java_obj.getMonotoneConstraints()

    assert constraints.apply("District") == -1.0
    assert constraints.apply("Group") == 1.0


def testMonotoneConstraintsGetProperlyPropagatedFromJavaBackend():
    gbm = H2OGBM(monotoneConstraints={"District": -1, "Group": 1})
    gbm._transfer_params_to_java()

    gbm.setMonotoneConstraints({"District": 1, "Group": -1})

    constraints = gbm.getMonotoneConstraints()
    assert constraints["District"] == 1.0
    assert constraints["Group"] == -1.0

    gbm._transfer_params_from_java()

    constraints = gbm.getMonotoneConstraints()
    assert constraints["District"] == -1.0
    assert constraints["Group"] == 1.0


def testLoadGBMModelAsMOJOModel(savedGbmModel):
    gbmModel = H2OMOJOModel.createFromMojo(savedGbmModel)
    assert gbmModel.getNtrees() > 0


def testLoadGBMModelAsSupervisedMOJOModel(savedGbmModel):
    gbmModel = H2OSupervisedMOJOModel.createFromMojo(savedGbmModel)
    assert gbmModel.getNtrees() > 0


def testLoadGBMModelAsTreeBasedSupervisedMOJOModel(savedGbmModel):
    gbmModel = H2OTreeBasedSupervisedMOJOModel.createFromMojo(savedGbmModel)
    assert gbmModel.getNtrees() > 0
