"""
Unit tests for PySparkling Mojo. We don't start H2O context for these tests to actually tests
that mojo can run without H2O runtime in PySparkling environment
"""

import os
import unit_test_utils
import pytest
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql.functions import col
from pyspark.sql import Row
from pysparkling.ml import H2OMOJOModel, H2OMOJOSettings


@pytest.fixture(scope="module")
def prostateDatasetWithBinomialPrediction(spark):
    return spark.read.csv("file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate_prediction.csv"),
                          header=True, inferSchema=True)


def testMojoPredictions(prostateDataset, prostateDatasetWithBinomialPrediction):
    # Try loading the Mojo and prediction on it without starting H2O Context
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))
    result = mojo.transform(prostateDataset)\
        .repartition(1)\
        .withColumn("prob0", col("detailed_prediction.probabilities.0")) \
        .withColumn("prob1", col("detailed_prediction.probabilities.1")) \
        .drop("detailed_prediction")
    unit_test_utils.assert_data_frames_are_identical(result, prostateDatasetWithBinomialPrediction)


def testMojoPredictionsUnseenCategoricals(spark):
    path = "file://" + os.path.abspath("../ml/src/test/resources/deep_learning_airlines_categoricals.zip")
    settings = H2OMOJOSettings(convertUnknownCategoricalLevelsToNa=True)
    mojo = H2OMOJOModel.createFromMojo(path, settings)

    rowForScoring = Row("sepal_len", "sepal_wid", "petal_len", "petal_wid", "class")

    df = spark.createDataFrame(spark.sparkContext.
                               parallelize([(5.1, 3.5, 1.4, 0.2, "Missing_categorical")]).
                               map(lambda r: rowForScoring(*r)))
    data = mojo.transform(df).collect()[0]

    assert data["class"] == "Missing_categorical"
    assert data["petal_len"] == 1.4
    assert data["petal_wid"] == 0.2
    assert data["sepal_len"] == 5.1
    assert data["sepal_wid"] == 3.5
    assert data["prediction"] == 5.240174068202646


def testMojoModelSerializationInPipeline(prostateDataset):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/binom_model_prostate.mojo"))

    pipeline = Pipeline(stages=[mojo])

    pipeline.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))
    loadedPipeline = Pipeline.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo"))

    model = loadedPipeline.fit(prostateDataset)

    model.write().overwrite().save("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))
    PipelineModel.load("file://" + os.path.abspath("build/test_spark_pipeline_model_mojo_model"))


def testMojoUnsupervised(spark):
    mojo = H2OMOJOModel.createFromMojo(
        "file://" + os.path.abspath("../ml/src/test/resources/isolation_forest.mojo"))

    rowForScoring = Row("V1")

    df = spark.createDataFrame(spark.sparkContext.
                               parallelize([(5.1,)]).
                               map(lambda r: rowForScoring(*r)))
    mojo.transform(df).repartition(1).collect()
