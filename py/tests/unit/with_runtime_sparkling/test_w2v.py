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
