from pysparkling import *
from pyspark.sql import SparkSession
import h2o

# Start Cluster
spark = SparkSession.builder.appName("App name").getOrCreate()
hc = H2OContext.getOrCreate()
assert h2o.cluster().cloud_size == 3

# Prepare Data
frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
sparkDF = hc.asSparkFrame(frame)
sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
[trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

# Train Model
from pysparkling.ml import H2OXGBoost
estimator = H2OXGBoost(labelCol = "CAPSULE")
model = estimator.fit(trainingDF)

# Run Predictions
model.transform(testingDF).collect()
