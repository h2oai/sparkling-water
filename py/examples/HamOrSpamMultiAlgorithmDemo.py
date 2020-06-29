import os

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import HashingTF, RegexTokenizer, StopWordsRemover, IDF
from pyspark.sql import SparkSession

from pysparkling import *
from pysparkling.ml import ColumnPruner, H2OGBM, H2ODeepLearning, H2OAutoML, H2OXGBoost

# Initiate SparkSession
spark = SparkSession.builder.appName("App name").getOrCreate()

hc = H2OContext.getOrCreate()

## This method loads the data, perform some basic filtering and create Spark's dataframe
def load():
    dataPath = "file://" + os.path.abspath("../examples/smalldata/smsData.txt")
    row_rdd = spark.sparkContext.textFile(dataPath).map(lambda x: x.split("\t", 1)).filter(lambda r: r[0].strip())
    return spark.createDataFrame(row_rdd, ["label", "text"])

##
## Define the pipeline stages
##

## Tokenize the messages
tokenizer = RegexTokenizer(inputCol="text",
                           outputCol="words",
                           minTokenLength=3,
                           gaps=False,
                           pattern="[a-zA-Z]+")

## Remove ignored words
stopWordsRemover = StopWordsRemover(inputCol=tokenizer.getOutputCol(),
                                    outputCol="filtered",
                                    stopWords=["the", "a", "", "in", "on", "at", "as", "not", "for"],
                                    caseSensitive=False)

## Hash the words
hashingTF = HashingTF(inputCol=stopWordsRemover.getOutputCol(),
                      outputCol="wordToIndex",
                      numFeatures=1 << 10)

## Create inverse document frequencies model
idf = IDF(inputCol=hashingTF.getOutputCol(),
          outputCol="tf_idf",
          minDocFreq=4)

gbm = H2OGBM(splitRatio=0.8,
             seed=1,
             featuresCols=[idf.getOutputCol()],
             labelCol="label")

dl = H2ODeepLearning(epochs=10,
                     seed=1,
                     l1=0.001,
                     l2=0.0,
                     hidden=[200, 200],
                     featuresCols=[idf.getOutputCol()],
                     labelCol="label")

automl = H2OAutoML(convertUnknownCategoricalLevelsToNa=True,
                   maxRuntimeSecs=60*100, # 100 minutes
                   maxModels=10,
                   seed=1,
                   labelCol="label")

xgboost = H2OXGBoost(convertUnknownCategoricalLevelsToNa=True,
                           featuresCols=[idf.getOutputCol()],
                           labelCol="label")

data = load()

def trainPipelineModel(idf, hashingTF, stopWordsRemover, tokenizer, algoStage, data):
    ## Remove all helper columns
    colPruner = ColumnPruner(columns=[idf.getOutputCol(), hashingTF.getOutputCol(), stopWordsRemover.getOutputCol(), tokenizer.getOutputCol()])

    ## Create the pipeline by defining all the stages
    pipeline = Pipeline(stages=[tokenizer, stopWordsRemover, hashingTF, idf, algoStage, colPruner])

    ## Test exporting and importing the pipeline. On Systems where HDFS & Hadoop is not available, this call store the pipeline
    ## to local file in the current directory. In case HDFS & Hadoop is available, this call stores the pipeline to HDFS home
    ## directory for the current user. Absolute paths can be used as wells. The same holds for the model import/export bellow.
    pipelinePath = "file://" + os.path.abspath("../build/pipeline")
    pipeline.write().overwrite().save(pipelinePath)
    loaded_pipeline = Pipeline.load(pipelinePath)

    ## Train the pipeline model
    modelPath = "file://" + os.path.abspath("../build/model")
    model = loaded_pipeline.fit(data)
    model.write().overwrite().save(modelPath)
    return PipelineModel.load(modelPath)


def isSpam(smsText, model):
    smsTextDF = spark.createDataFrame([(smsText,)], ["text"]) # create one element tuple
    prediction = model.transform(smsTextDF)
    return prediction.select("prediction").first()["prediction"] == "spam"


def assertPredictions(model):
    isSpamMsg = isSpam("Michal, h2oworld party tonight in MV?", model)
    assert not isSpamMsg
    print(isSpamMsg)

    isSpamMsg = isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", model)
    assert isSpamMsg
    print(isSpamMsg)

estimators = [gbm, dl, automl, xgboost]
for estimator in estimators:
    model = trainPipelineModel(idf, hashingTF, stopWordsRemover, tokenizer, estimator, data)
    assertPredictions(model)
