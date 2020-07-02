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

import ai.h2o.sparkling.ml.algos.{H2OGBM, H2OGLM, H2OSupervisedAlgorithm}
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import _root_.hex.Model
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val df = if (context.datasetDetails.isVirtual) {
      loadVirtualDataFrame()
    } else {
      loadRegularDataFrame()
    }

    val persistedDF = df.persist(StorageLevel.DISK_ONLY)
    persistedDF.foreach(_ => {}) // Load DataFrame to cache.
    persistedDF
  }

  private def loadRegularDataFrame(): DataFrame = {
    context.spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(context.datasetDetails.url.get)
  }

  private def generateVirtualColumns(): Seq[String] = {
    val numberOfColumns = context.datasetDetails.nCols.get
    require(numberOfColumns > 0, "Number of columns must be a positive number.")
    context.datasetDetails.labelCol +: (1 until numberOfColumns).map(i => "feature_" + i)
  }

  private def loadVirtualDataFrame(): DataFrame = {
    val columns = generateVirtualColumns()
    val minValue: Long = context.datasetDetails.minValue.getOrElse[Int](Int.MinValue)
    val maxValue: Long = context.datasetDetails.maxValue.getOrElse[Int](Int.MaxValue)
    val rangeSize = maxValue - minValue
    val initialDF = context.spark.range(
      start = 0,
      end = context.datasetDetails.nRows.get,
      step = 1,
      numPartitions = context.datasetDetails.nPartitions.getOrElse(200))
    initialDF.select(columns.map(c => ((rand() * lit(rangeSize)) + lit(minValue)).cast(IntegerType).as(c)): _*)
  }

  def removeFromCache(dataFrame: DataFrame): Unit = dataFrame.unpersist(blocking = true)

  def loadDataToH2OFrame(): H2OFrame = {
    if (context.datasetDetails.isVirtual) {
      loadVirtualH2OFrame()
    } else {
      loadRegularH2OFrame()
    }
  }

  def loadRegularH2OFrame(): H2OFrame = {
    val uri = new URI(context.datasetDetails.url.get)
    val frame = H2OFrame(uri)
    frame
  }

  def loadVirtualH2OFrame(): H2OFrame = {
    val numberOfRows = context.datasetDetails.nRows.get
    val numberOfPartitions = context.datasetDetails.nPartitions.getOrElse(200)
    val minValue: Long = context.datasetDetails.minValue.getOrElse[Int](Int.MinValue)
    val maxValue: Long = context.datasetDetails.maxValue.getOrElse[Int](Int.MaxValue)
    val rangeSize = maxValue - minValue
    val columns = generateVirtualColumns().toArray
    def getNextRandomNumber: Int = {
      val randDouble = scala.util.Random.nextDouble()
      ((randDouble * rangeSize) + minValue).asInstanceOf[Int]
    }
    val nextRandomNumberUdf = udf(getNextRandomNumber _)
    var initialDS = SparkSession.active.range(0, numberOfRows, 1, numberOfPartitions).toDF("drop")
    columns.foreach { col =>
      initialDS = initialDS.withColumn(col, nextRandomNumberUdf())
    }
    H2OContext.ensure().asH2OFrame(initialDS.drop("drop"))
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
    s"${super.getResultHeader()} and algorithm '${algorithm.h2oAlgorithm._1}'"
  }
}

object AlgorithmBenchmarkBase {
  val supportedAlgorithms: Seq[AlgorithmBundle] = {
    Seq(
      AlgorithmBundle(new H2OGBM, ("gbm", Map.empty[String, String])),
      AlgorithmBundle(new H2OGLM, ("glm", Map.empty[String, String])))
  }
}

case class AlgorithmBundle(
    swAlgorithm: H2OSupervisedAlgorithm[_ <: Model.Parameters],
    h2oAlgorithm: (String, Map[String, String])) {
  def newInstance(): AlgorithmBundle = {
    val clonedSwAlgorithm = swAlgorithm.copy(ParamMap.empty)
    AlgorithmBundle(clonedSwAlgorithm, h2oAlgorithm)
  }
}

case class Measurement(id: Int, name: String, value: Any)

case class DatasetDetails(
    name: String,
    isVirtual: Boolean,
    labelCol: String,
    url: Option[String],
    nCols: Option[Int],
    nRows: Option[Int],
    nPartitions: Option[Int],
    minValue: Option[Int],
    maxValue: Option[Int])

case class BenchmarkContext(spark: SparkSession, hc: H2OContext, datasetDetails: DatasetDetails)
