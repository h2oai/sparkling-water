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

// Scoring-only smoke test: MOJO load, scoring and pipeline persistence without an H2OContext.
// Inputs (Spark conf): spark.sw.scoring.test.mojo, spark.sw.scoring.test.data, spark.sw.scoring.test.out

import ai.h2o.sparkling.ml.models.H2OMOJOModel
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{abs, col}

val spark = SparkSession.builder.getOrCreate()
val Tolerance = 1e-6

def testConf(key: String): String = spark.conf.get(s"spark.sw.scoring.test.$key")

val data = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv(testConf("data"))
  .withColumnRenamed("prediction", "expected_prediction")
  .withColumnRenamed("prob0", "expected_prob0")
  .withColumnRenamed("prob1", "expected_prob1")

def check(model: Transformer, input: DataFrame, label: String): Unit = {
  val scored = model.transform(input)
  val mismatches = scored.where(
    col("prediction").cast("string") =!= col("expected_prediction").cast("string") ||
      abs(col("detailed_prediction.probabilities.0") - col("expected_prob0")) > Tolerance ||
      abs(col("detailed_prediction.probabilities.1") - col("expected_prob1")) > Tolerance)
  val (rows, bad) = (scored.count(), mismatches.count())
  if (rows != input.count() || bad != 0) {
    throw new AssertionError(s"$label: $bad of $rows rows differ from expected predictions")
  }
  println(s"$label: $rows rows match expected predictions")
}

def h2oContextExists(): Boolean =
  try {
    val companion = Class.forName("ai.h2o.sparkling.H2OContext$").getField("MODULE$").get(null)
    companion.getClass.getMethod("get").invoke(companion).asInstanceOf[Option[_]].isDefined
  } catch {
    case _: ClassNotFoundException => false
  }

def scoringArtifact(): String =
  Option(classOf[H2OMOJOModel].getProtectionDomain.getCodeSource)
    .map(source => new java.io.File(source.getLocation.getPath).getName)
    .getOrElse("unknown")

val model = H2OMOJOModel.createFromMojo(testConf("mojo"))
check(model, data, "MOJO model")

val pipelinePath = testConf("out") + "/scala-pipeline"
new Pipeline().setStages(Array[PipelineStage](model)).fit(data).write.overwrite().save(pipelinePath)
check(PipelineModel.load(pipelinePath), data, "Reloaded pipeline")

if (h2oContextExists()) throw new AssertionError("An H2OContext was created; scoring must not need one")

println(
  s"Sparkling Water ${scoringArtifact()}, Spark ${spark.version}, " +
    s"Scala ${scala.util.Properties.versionNumberString}, JDK ${System.getProperty("java.version")}")
println("SCORING TEST PASSED")
