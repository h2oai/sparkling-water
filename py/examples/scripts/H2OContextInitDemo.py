from pysparkling import *
from pyspark.sql import SparkSession
import h2o

# Initiate SparkSession
spark = SparkSession.builder.appName("App name").getOrCreate()

# Initiate H2OContext
hc = H2OContext.getOrCreate(spark)

# Stop H2O and Spark services
h2o.cluster().shutdown()
spark.stop()