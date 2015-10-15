import pysparkling
from pyspark import SparkContext
from pyspark.sql import SQLContext

# initiate SparkContext
sc = SparkContext("local", "App Name", pyFiles=[])

# initiate SQLContext
sqlContext = SQLContext(sc)

# initiate H2OContext
hc = pysparkling.H2OContext(sc).start()