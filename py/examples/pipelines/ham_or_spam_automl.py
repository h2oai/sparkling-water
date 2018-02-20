from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, RegexTokenizer, StopWordsRemover, IDF
from pysparkling import *
from pysparkling.ml import ColumnPruner, H2OAutoML
import os

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

## Create H2OAutoML model
automl = H2OAutoML(convertUnknownCategoricalLevelsToNa=False,
                   maxRuntimeSecs=300, # 5 minutes
                   predictionCol="label")

## Remove all helper columns
colPruner = ColumnPruner(columns=[idf.getOutputCol(), hashingTF.getOutputCol(), stopWordsRemover.getOutputCol(), tokenizer.getOutputCol()])

## Create the pipeline by defining all the stages
pipeline = Pipeline(stages=[tokenizer, stopWordsRemover, hashingTF, idf, automl, colPruner])

## Train the pipeline model
data = load()
model = pipeline.fit(data)

##
## Make predictions on unlabeled data
## Spam detector
##
def isSpam(smsText, model, hamThreshold = 0.5):
    smsTextDF = spark.createDataFrame([(smsText,)], ["text"]) # create one element tuple
    prediction = model.transform(smsTextDF)
    return prediction.select("prediction_output.p1").first()["p1"] > hamThreshold


print(isSpam("Michal, h2oworld party tonight in MV?", model))

print(isSpam("We tried to contact you re your reply to our offer of a Video Handset? 750 anytime any networks mins? UNLIMITED TEXT?", model))
