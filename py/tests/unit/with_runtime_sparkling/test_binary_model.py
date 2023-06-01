import h2o
from h2o.estimators.estimator_base import H2OEstimator
from pyspark.mllib.linalg import *
from pysparkling.ml import *


def testModelIsLoadedOnBackend(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8,
                  keepBinaryModels=True)
    algo.fit(prostateDataset)
    algo.getBinaryModel().write("build/binary.model")
    swBinaryModel = H2OBinaryModel.read("build/binary.model")
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLM(prostateDataset):
    algo = H2OGLM(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                  labelCol="AGE",
                  seed=1,
                  splitRatio=0.8,
                  keepBinaryModels=True)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLMClassifier(prostateDataset):
    algo = H2OGLMClassifier(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                            labelCol="AGE",
                            seed=1,
                            splitRatio=0.8,
                            keepBinaryModels=True)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)


def testModelIsLoadedOnBackendWhenTrainedOnGLMRegressor(prostateDataset):
    algo = H2OGLMRegressor(featuresCols=["CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"],
                           labelCol="AGE",
                           seed=1,
                           splitRatio=0.8,
                           keepBinaryModels=True)
    algo.fit(prostateDataset)
    swBinaryModel = algo.getBinaryModel()
    h2oBinaryModel = h2o.get_model(swBinaryModel.modelId)
    assert isinstance(h2oBinaryModel, H2OEstimator)
