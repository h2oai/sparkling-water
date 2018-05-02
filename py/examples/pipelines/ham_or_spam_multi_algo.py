import os
import sys

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import HashingTF, RegexTokenizer, StopWordsRemover, IDF
from pyspark.sql import SparkSession

from pysparkling import *
from pysparkling.ml import ColumnPruner, H2OGBM, H2ODeepLearning, H2OAutoML

# Determine which algorithm to use, if no specified as argument, gbm is used
if len(sys.argv) != 2 or sys.argv[1] not in ["gbm", "dl", "automl"]:
    algo="gbm"
else:
    algo=sys.argv[1]

# Initiate SparkSession
spark = SparkSession.builder.appName("App name").getOrCreate()

hc = H2OContext.getOrCreate(spark)

# This is just helper function returning path to data-files
def _locate(file_name):
    if os.path.isfile("/home/0xdiag/smalldata/" + file_name):
        return "file:///home/0xdiag/smalldata/" + file_name
    else:
        return "../examples/smalldata/" + file_name

## This method loads the data, perform some basic filtering and create Spark's dataframe
def load():
    row_rdd = spark.sparkContext.textFile(_locate("smsData.txt")).map(lambda x: x.split("\t", 1)).filter(lambda r: r[0].strip())
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


if algo == "gbm":
    ## Create GBM model
    algoStage = H2OGBM(ratio=0.8,
                 seed=1,
                 featuresCols=[idf.getOutputCol()],
                 predictionCol="label")
elif algo == "dl":
    ## Create H2ODeepLearning model
    algoStage = H2ODeepLearning(epochs=10,
                         seed=1,
                         l1=0.001,
                         l2=0.0,
                         hidden=[200, 200],
                         featuresCols=[idf.getOutputCol()],
                         predictionCol="label")
elif algo == "automl":
    ## Create H2OAutoML model
    algoStage = H2OAutoML(convertUnknownCategoricalLevelsToNa=False,
                       maxRuntimeSecs=60, # 1 minutes
                       seed=1,
                       predictionCol="label")

## Remove all helper columns
colPruner = ColumnPruner(columns=[idf.getOutputCol(), hashingTF.getOutputCol(), stopWordsRemover.getOutputCol(), tokenizer.getOutputCol()])

## Create the pipeline by defining all the stages
pipeline = Pipeline(stages=[tokenizer, stopWordsRemover, hashingTF, idf, algoStage, colPruner])

## Test exporting and importing the pipeline. On Systems where HDFS & Hadoop is not available, this call store the pipeline
## to local file in the current directory. In case HDFS & Hadoop is available, this call stores the pipeline to HDFS home
## directory for the current user. Absolute paths can be used as wells. The same holds for the model import/export bellow.
pipeline.write().overwrite().save("examples/build/pipeline")
loaded_pipeline = Pipeline.load("examples/build/pipeline")

## Train the pipeline model
data = load()
model = loaded_pipeline.fit(data)

model.write().overwrite().save("examples/build/model")
loaded_model = PipelineModel.load("examples/build/model")




##
## Make predictions on unlabeled data
## Spam detector
##
def isSpam(smsText, model, hamThreshold = 0.5):
    smsTextDF = spark.createDataFrame([(smsText,)], ["text"]) # create one element tuple
    prediction = model.transform(smsTextDF)
    return prediction.select("prediction_output.p1").first()["p1"] > hamThreshold


print(isSpam("Michal, h2oworld party tonight in MV?", loaded_model))

print(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", loaded_model))
