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
import java.net.URI

import ai.h2o.sparkling.ml.algos.H2OSupervisedAlgorithm
import hex.glm.GLM
import hex.glm.GLMModel.GLMParameters
import hex.tree.gbm.GBM
import hex.tree.gbm.GBMModel.GBMParameters
import hex.{Model, ModelBuilder}
import org.apache.spark.h2o.{H2OBaseModel, H2OBaseModelBuilder, H2OFrame}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.ml.algos.{H2OGBM, H2OGLM}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

abstract class BenchmarkBase[TInput](context: BenchmarkContext) {
  private var lastMeasurementId = 1
  private val measurements = new ArrayBuffer[Measurement]()

  protected def addMeasurement(name: String, value: Any): Unit = {
    lastMeasurementId = lastMeasurementId + 1
    measurements.append(Measurement(lastMeasurementId, name, value))
  }

  protected def getResultHeader(): String = {
    s"${this.getClass.getSimpleName} results for the dataset '${context.datasetDetails.name}'"
  }

  protected def initialize(): TInput

  protected def body(input: TInput): Unit

  protected def cleanUp(input: TInput): Unit = {}

  def loadDataToDataFrame(): DataFrame = {
    val df = context.spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(context.datasetDetails.url)
      .persist(StorageLevel.DISK_ONLY)

    df.foreach(_ => {}) // Load DataFrame to cache.

    df
  }

  def removeFromCache(dataFrame: DataFrame): Unit = dataFrame.unpersist(blocking = true)

  def loadDataToH2OFrame(): H2OFrame = {
    val uri = new URI(context.datasetDetails.url)
    val frame = new H2OFrame(uri)
    frame
  }

  def run(): Unit = {
    val input = initialize()
    val startedAtNanos = System.nanoTime()
    body(input)
    val elapsedAtNanos = System.nanoTime() - startedAtNanos
    val durationAtNanos = Duration.fromNanos(elapsedAtNanos)
    val duration = Duration(durationAtNanos.toMillis, MILLISECONDS)
    measurements.append(Measurement(1, "time", duration))
    cleanUp(input)
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

abstract class AlgorithmBenchmarkBase[TInput](context: BenchmarkContext, algorithm: AlgorithmBundle)
  extends BenchmarkBase[TInput](context) {

  override protected def getResultHeader(): String = {
    s"${super.getResultHeader()} and algorithm '${algorithm.h2oAlgorithm.getClass.getSimpleName}'"
  }
}

object AlgorithmBenchmarkBase {
  val supportedAlgorithms: Seq[AlgorithmBundle] = {
    Seq(
      AlgorithmBundle(new H2OGBM, new GBM(new GBMParameters)),
      AlgorithmBundle(new H2OGLM, new GLM(new GLMParameters)))
  }
}

case class AlgorithmBundle(
    swAlgorithm: H2OSupervisedAlgorithm[_ <: H2OBaseModelBuilder, _ <: H2OBaseModel, _ <: Model.Parameters],
    h2oAlgorithm: ModelBuilder[_, _ <: Model.Parameters, _]) {
  def newInstance(): AlgorithmBundle = {
    val clonedSwAlgorithm = swAlgorithm.copy(ParamMap.empty)
    val clonedH2OParams = h2oAlgorithm._parms.clone()
    val h2oAlgorithmClass = h2oAlgorithm.getClass
    val constructor = h2oAlgorithmClass.getConstructor(clonedH2OParams.getClass)
    val newH2OAlgorithm = constructor.newInstance(clonedH2OParams)
    AlgorithmBundle(clonedSwAlgorithm, newH2OAlgorithm)
  }
}

case class Measurement(id: Int, name: String, value: Any)

case class DatasetDetails(name: String, url: String, labelCol: String)

case class BenchmarkContext(spark: SparkSession, hc: H2OContext, datasetDetails: DatasetDetails)
