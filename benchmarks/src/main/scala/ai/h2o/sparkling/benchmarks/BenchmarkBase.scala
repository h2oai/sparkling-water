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

package ai.h2o.sparkling.benchmarks

import java.io.{OutputStream, PrintWriter}

import ai.h2o.sparkling.ml.algos.H2OAlgorithm
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.{H2OGBM, H2OGLM}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

abstract class BenchmarkBase(context: BenchmarkContext) {
  private var lastMeasurementId = 1
  private val measurements = new ArrayBuffer[Measurement]()

  protected def addMeasurement(name: String, value: Any): Unit = {
    lastMeasurementId = lastMeasurementId + 1
    measurements.append(Measurement(lastMeasurementId, name, value))
  }

  protected def getResultHeader(): String = {
    s"${this.getClass.getSimpleName} results for the dataset '${context.datasetDetails.name}'"
  }

  protected def body(): Unit

  def run(): Unit = {
    val startedAtNanos = System.nanoTime()
    body()
    val elapsedAtNanos = System.nanoTime() - startedAtNanos
    val durationAtNanos = Duration.fromNanos(elapsedAtNanos)
    val duration = Duration(durationAtNanos.toSeconds, SECONDS)
    measurements.append(Measurement(1, "time", duration))
  }

  def exportMeasurements(outputStream: OutputStream): Unit = {
    val sortedMeasurements = measurements.sortBy(_.id)
    val writer = new PrintWriter(outputStream, true)
    writer.println(getResultHeader() + ":")
    for (Measurement(_, name, value) <- sortedMeasurements) {
      writer.println(s"$name: $value")
    }
    writer.println()
  }
}

abstract class AlgorithmBenchmarkBase(context: BenchmarkContext, algorithm: H2OAlgorithm[_, _, _])
  extends BenchmarkBase(context) {

  override protected def getResultHeader(): String = {
    s"${super.getResultHeader()} and algorithm '${algorithm.getClass.getSimpleName}'"
  }
}

object AlgorithmBenchmarkBase {
  val supportedAlgorithms: Seq[H2OAlgorithm[_, _, _]] = Seq(new H2OGBM, new H2OGLM)
}

case class Measurement(id: Int, name: String, value: Any)

case class DatasetDetails(name: String, url: String, labelCol: String)

case class BenchmarkContext(spark: SparkSession, hc: H2OContext, datasetDetails: DatasetDetails)
