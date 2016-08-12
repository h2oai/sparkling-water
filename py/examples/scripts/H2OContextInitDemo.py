from pysparkling import *
from pyspark import SparkContext
from pyspark.sql import SQLContext
import h2o
# initiate SparkContext
sc = SparkContext("local", "App Name", pyFiles=[])

# initiate SQLContext
sqlContext = SQLContext(sc)

# initiate H2OContext
hc = H2OContext.getOrCreate(sc)

# stop H2O and Spark services
h2o.shutdown(prompt=False)
sc.stop()