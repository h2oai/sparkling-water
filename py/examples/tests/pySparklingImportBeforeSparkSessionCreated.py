from pysparkling import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("App name").getOrCreate()
# Check if Sparkling Water classes are available
jvm = spark.sparkContext._jvm
package = getattr(jvm.ai.h2o.sparkling.backend, "BuildInfo$")
module = package.__getattr__("MODULE$")
# This would fail if the PYSPARK_SUBMIT_ARGS would not specify jar dependency to SW as it would not be able to find the class
module.SWVersion()
