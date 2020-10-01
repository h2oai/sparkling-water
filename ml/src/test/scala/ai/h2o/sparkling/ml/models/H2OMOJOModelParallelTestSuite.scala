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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.{SparkTestContext, TestUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OMOJOModelParallelTestSuite extends FunSuite with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val prostateDataFrame: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("examples/smalldata/prostate/prostate.csv")

  private def scoreWithBinomialModel(): DataFrame = {
    val model = H2OMOJOModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("binom_model_prostate.mojo"),
      "binom_model_prostate.mojo")
    model.transform(prostateDataFrame)
  }

  test("Score with Binomial MOJO model in parallel") {
    val numberOfThreads = 10
    val results = new Array[DataFrame](numberOfThreads)
    val exceptions = new Array[Throwable](numberOfThreads)

    val threads = for (i <- 0 until numberOfThreads) yield {
      // Scala Futures could be used here, but they don't guarantee the parallel execution.
      val thread = new Thread {
        override def run(): Unit = {
          results(i) = scoreWithBinomialModel()
        }
      }
      thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler {
        override def uncaughtException(t: Thread, exception: Throwable): Unit = {
          exceptions(i) = exception
        }
      })
      thread.start()
      thread
    }

    threads.foreach(_.join())

    for ((exception, i) <- exceptions.zipWithIndex if exception != null) {
      throw new AssertionError(s"The thread $i has thrown an exception.", exception)
    }

    val referenceDataset = scoreWithBinomialModel()
    results.foreach(TestUtils.assertDataFramesAreIdentical(referenceDataset, _))
  }
}
