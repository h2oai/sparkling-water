/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Training smoke test: internal backend start, H2O frame import and conversion, XGBoost training and prediction.
// Needs ../timing.scala loaded first (the runners do this). Spark conf spark.sw.training.test.nodes: expected H2O nodes,
// or "executors" on autoscaling clusters: at least 1 and at most the Spark executors present after the start
// (H2O counts the executors registered when it starts, and a worker can still join while it does).

import java.net.URI
import ai.h2o.sparkling._
import ai.h2o.sparkling.ml.algos.H2OXGBoost
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

val spark = timed("spark_session") { SparkSession.builder.appName("App name").getOrCreate() }
import spark.implicits._

val hc = timed("h2o_context") { H2OContext.getOrCreate() }
val executors = spark.sparkContext.statusTracker.getExecutorInfos.length - 1 // without the driver
val actual = hc.getH2ONodes().length
spark.conf.get("spark.sw.training.test.nodes", "3") match {
  case "executors" =>
    if (actual < 1 || actual > executors) {
      throw new RuntimeException(s"H2O cluster should have 1 to $executors nodes but has $actual")
    }
  case nodes =>
    if (actual != nodes.toInt) throw new RuntimeException(s"H2O cluster should be of size $nodes but is $actual")
}

val frame = timed("data_import") {
  spark.sparkContext.addFile(
    "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
  H2OFrame(new URI("file://" + SparkFiles.get("prostate.csv")))
}
val sparkDF = timed("as_spark_frame") {
  val df = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string").cache()
  df.count()
  df
}
// Fixed seed so every timing run trains and predicts on the same rows; its high bits (the fractional bits of
// sqrt(2)) give the generators well-mixed state. XGBoost passes seed % Int.MaxValue to the native library.
val seed = 0x6A09E667F3BCC908L
val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2), seed)

val model = timed("train") { new H2OXGBoost().setLabelCol("CAPSULE").setSeed(seed).fit(trainingDF) }
val predictions = timed("predict") { model.transform(testingDF).collect() }
if (predictions.isEmpty) throw new AssertionError("No predictions")

val summary = s"Sparkling Water ${ai.h2o.sparkling.backend.BuildInfo.SWVersion}, H2O ${water.H2O.ABV.projectVersion()}, $actual H2O nodes, $executors executors, " +
  s"Spark ${spark.version}, Scala ${scala.util.Properties.versionNumberString}, JDK ${System.getProperty("java.version")}"
println(summary)
println("TRAINING TEST PASSED")
