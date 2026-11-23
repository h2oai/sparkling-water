#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Training smoke test: internal backend start, H2O frame import and conversion, XGBoost training and prediction.
# Needs ../timing.py loaded first (the runners do this). Spark conf spark.sw.training.test.nodes: expected H2O nodes,
# or "executors" for the Spark executors present after the start (autoscaling clusters).

import platform

import h2o
from pyspark.sql import SparkSession
from pysparkling import H2OContext
from pysparkling.ml import H2OXGBoost

with timed("spark_session"):
    spark = SparkSession.builder.appName("App name").getOrCreate()
with timed("h2o_context"):
    hc = H2OContext.getOrCreate()
executors = len(spark.sparkContext._jsc.sc().statusTracker().getExecutorInfos()) - 1  # without the driver
nodes = spark.conf.get("spark.sw.training.test.nodes", "3")
expected = executors if nodes == "executors" else int(nodes)
actual = h2o.cluster().cloud_size
assert actual == expected, "H2O cluster should be of size {} but is {}".format(expected, actual)

with timed("data_import"):
    frame = h2o.import_file(
        "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
with timed("as_spark_frame"):
    sparkDF = hc.asSparkFrame(frame)
    sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string")).cache()
    sparkDF.count()
# Fixed seed so every timing run trains and predicts on the same rows; its high bits (the fractional bits of
# sqrt(2)) give the generators well-mixed state. XGBoost passes seed % Int.MaxValue to the native library.
seed = 0x6A09E667F3BCC908
[trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2], seed=seed)

with timed("train"):
    model = H2OXGBoost(labelCol="CAPSULE", seed=seed).fit(trainingDF)
with timed("predict"):
    predictions = model.transform(testingDF).collect()
assert predictions, "No predictions"

jvm = spark.sparkContext._jvm
summary = "Sparkling Water {}, H2O {}, {} H2O nodes, {} executors, Spark {}, Scala {}, JDK {}, Python {}".format(
    jvm.ai.h2o.sparkling.backend.BuildInfo.SWVersion(), h2o.__version__, actual, executors, spark.version,
    jvm.scala.util.Properties.versionNumberString(), jvm.java.lang.System.getProperty("java.version"),
    platform.python_version())
print(summary)
print("TRAINING TEST PASSED")
