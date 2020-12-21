from pysparkling import *
from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder.appName("App name").master("local").getOrCreate()
# Check if Sparkling Water classes are available
jvm = spark.sparkContext._jvm
scoringPackage = getattr(jvm.ai.h2o.sparkling.ml.models, "H2OMOJOProps$")
scoringModule = scoringPackage.__getattr__("MODULE$")

# This assert would fail if scoring jar wasn't loaded
assert scoringModule.serializedFileName() == "mojo_model"

# Make sure that the big assembly for model training is not loaded
try:
    package = getattr(jvm.ai.h2o.sparkling.backend, "BuildInfo$")
    module = package.__getattr__("MODULE$")
    module.SWVersion()
    sys.exit(1)
except:
    sys.exit(0)
