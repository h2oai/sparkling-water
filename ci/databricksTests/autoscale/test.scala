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

// Autoscaling probe for the internal backend: makes Databricks add or remove workers around a running H2OContext and
// records what the H2O cloud does. Needs ../timing.scala loaded first (the runners do this). Spark conf:
//   spark.sw.autoscale.test.scenario  up: start H2O on the workers present, load Spark until workers are added, train
//                                     down: load Spark until workers are added, start H2O on all of them, stay idle
//                                     until workers are removed (H2O nodes go with them), train
//   spark.sw.autoscale.test.timeout   seconds to wait for the scaling event (default 900)
// A missing scaling event or a failed training before it fails the probe. What H2O does after the event is the result
// under test, so it goes into the summary instead of an assertion. Training data comes from the driver, not from an
// H2O frame, so Spark can recompute it on any executor.

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}
import ai.h2o.sparkling._
import ai.h2o.sparkling.backend.utils.RestApiUtils
import ai.h2o.sparkling.ml.algos.H2OXGBoost
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()
import spark.implicits._
val sc = spark.sparkContext
val scenario = spark.conf.get("spark.sw.autoscale.test.scenario", "up")
require(Set("up", "down")(scenario), s"Unknown scenario $scenario (up or down)")
val timeout = spark.conf.get("spark.sw.autoscale.test.timeout", "900").toInt.seconds
val trainTimeout = 300.seconds
val seed = 0x6A09E667F3BCC908L // as in ../training/test.scala

def executors(): Int = sc.statusTracker.getExecutorInfos.length - 1 // without the driver

// Polls every 5 s until the executor count meets the condition; fails when it does not within the timeout.
def awaitExecutors(event: String)(condition: Int => Boolean): Int = {
  val deadline = timeout.fromNow
  while (!condition(executors())) {
    if (deadline.isOverdue()) throw new AssertionError(s"No $event within $timeout: ${executors()} executors")
    Thread.sleep(5000)
  }
  executors()
}

// Runs more sleeping Spark tasks than the cluster has cores until Databricks adds a worker, then cancels them.
def scaleUp(): Int = {
  val before = executors()
  Future {
    sc.setJobGroup("autoscale-load", "Load that makes Databricks add workers", interruptOnCancel = true)
    sc.parallelize(1 to 1000, 1000).foreach(_ => Thread.sleep(60000))
  }
  try awaitExecutors("scale-up")(_ > before)
  finally sc.cancelJobGroup("autoscale-load")
}

val lines = timed("data_read") {
  scala.io.Source
    .fromURL("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
    .getLines()
    .toList
}
val data = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(lines.toDS())
  .withColumn("CAPSULE", $"CAPSULE" cast "string")
val Array(trainingDF, testingDF) = data.randomSplit(Array(0.8, 0.2), seed)

// Trains and predicts within trainTimeout; describes the outcome instead of throwing.
def train(): String = {
  val xgboost = new H2OXGBoost().setLabelCol("CAPSULE").setSeed(seed)
  Try(Await.result(Future(xgboost.fit(trainingDF).transform(testingDF).count()), trainTimeout)) match {
    case Success(rows) => s"succeeded ($rows predictions)"
    case Failure(_: TimeoutException) => s"gave no result within $trainTimeout"
    case Failure(e) => s"failed (${e.getClass.getName}: ${String.valueOf(e.getMessage).take(300)})"
  }
}

// The H2O cloud as its leader reports it over REST (the driver is not an H2O node), and the H2O nodes whose host has no
// Spark executor anymore.
def cloud(hc: H2OContext): String = {
  val state = Try(RestApiUtils.getPingInfo(hc.getConf)) match {
    case Success(ping) => s"${ping.nodes.length} nodes, healthy ${ping.cloud_healthy}"
    case Failure(e) => s"not reachable (${e.getClass.getSimpleName})"
  }
  val hosts = sc.statusTracker.getExecutorInfos.map(_.host).toSet
  val lost = hc.getH2ONodes().count(node => !hosts(node.hostname))
  s"$state; $lost of ${hc.getH2ONodes().length} H2O nodes lost their executor"
}

val startExecutors = executors()
if (scenario == "down") timed("scale_up")(scaleUp())
val hc = timed("h2o_context")(H2OContext.getOrCreate())
val h2oNodes = hc.getH2ONodes().length
val h2oExecutors = executors()
val trainedBefore = timed("train")(train())
if (!trainedBefore.startsWith("succeeded")) throw new AssertionError(s"Training before the event $trainedBefore")

val (event, eventExecutors) = scenario match {
  case "up" => ("scale-up", timed("scale_up")(scaleUp()))
  case _ =>
    val peak = executors()
    ("scale-down", timed("scale_down")(awaitExecutors("scale-down")(_ < peak)))
}
val trainedAfter = timed("train_after")(train())

val summary = s"Scenario $scenario: $startExecutors executors at start; H2O cloud of $h2oNodes nodes started with " +
  s"$h2oExecutors executors; training $trainedBefore; $event to $eventExecutors executors; training after it " +
  s"$trainedAfter; then ${executors()} executors, H2O cloud ${cloud(hc)}"
println(summary)
println("AUTOSCALE PROBE PASSED")
