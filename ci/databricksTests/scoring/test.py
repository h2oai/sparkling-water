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
# Scoring-only smoke test: MOJO load, scoring and pipeline persistence without an H2OContext.
# Inputs (Spark conf): spark.sw.scoring.test.mojo, spark.sw.scoring.test.data, spark.sw.scoring.test.out

import pkgutil
import platform

from py4j.java_gateway import JavaClass
from pyspark.ml import Pipeline, PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pysparkling.ml import H2OMOJOModel

TOLERANCE = 1e-6
spark = SparkSession.builder.getOrCreate()
jvm = spark.sparkContext._jvm


def test_conf(key):
    return spark.conf.get("spark.sw.scoring.test." + key)


def check(model, data, label):
    scored = model.transform(data)
    mismatches = scored.where(
        (F.col("prediction").cast("string") != F.col("expected_prediction").cast("string"))
        | (F.abs(F.col("detailed_prediction.probabilities.0") - F.col("expected_prob0")) > TOLERANCE)
        | (F.abs(F.col("detailed_prediction.probabilities.1") - F.col("expected_prob1")) > TOLERANCE))
    rows, bad = scored.count(), mismatches.count()
    if rows != data.count() or bad != 0:
        raise AssertionError("{}: {} of {} rows differ from expected predictions".format(label, bad, rows))
    print("{}: {} rows match expected predictions".format(label, rows))


def h2o_context_exists():
    h2o_context = jvm.ai.h2o.sparkling.H2OContext
    return isinstance(h2o_context, JavaClass) and h2o_context.get().isDefined()


data = (spark.read.option("header", "true").option("inferSchema", "true").csv(test_conf("data"))
        .withColumnRenamed("prediction", "expected_prediction")
        .withColumnRenamed("prob0", "expected_prob0")
        .withColumnRenamed("prob1", "expected_prob1"))

model = H2OMOJOModel.createFromMojo(test_conf("mojo"))
check(model, data, "MOJO model")

pipeline_path = test_conf("out") + "/python-pipeline"
Pipeline(stages=[model]).fit(data).write().overwrite().save(pipeline_path)
check(PipelineModel.load(pipeline_path), data, "Reloaded pipeline")

if h2o_context_exists():
    raise AssertionError("An H2OContext was created; scoring must not need one")

print("Sparkling Water {}, Spark {}, Scala {}, JDK {}, Python {}".format(
    pkgutil.get_data("ai.h2o.sparkling", "version.txt").decode().strip(), spark.version,
    jvm.scala.util.Properties.versionNumberString(), jvm.java.lang.System.getProperty("java.version"),
    platform.python_version()))
print("SCORING TEST PASSED")
