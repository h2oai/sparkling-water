from pysparkling import *
from pyspark.sql import SparkSession

# Initiate SparkSession
spark = SparkSession.builder.appName("App name").getOrCreate()

# Initiate H2OContext
hc = H2OContext.getOrCreate(spark)
