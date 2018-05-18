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

package org.apache.spark.ml.h2o.models

import java.io._

import ai.h2o.mojos.runtime.MojoPipeline
import ai.h2o.mojos.runtime.frame.MojoColumn
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.reflect.ClassTag


class H2OMOJOPipelineModel(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMOJOPipelineModel] with MLWritable {

  @transient private var model: MojoPipeline = _

  def getOrCreateModel() = {
    if (model == null) {
      val reader = MojoPipelineReaderBackendFactory.createReaderBackend(new ByteArrayInputStream(mojoData))
      model = MojoPipeline.loadFrom(reader)
    }
    model
  }

  def this(mojoData: Array[Byte]) = this(mojoData, Identifiable.randomUID("mojoPipelineModel"))

  val outputCol = "prediction"

  case class Mojo2Prediction(preds: List[String])

  private val modelUdf = (names: Array[String]) =>
    udf[Mojo2Prediction, Row] {
      r: Row =>
        val m = getOrCreateModel()
        val builder = m.getInputFrameBuilder
        val rowBuilder = builder.getMojoRowBuilder
        val filtered = r.getValuesMap[Any](names).filter { case (n, _) => m.getInputMeta.contains(n) }
        filtered.foreach {
          case (colName, colData) =>
            val data = if (colData == null) {
              null
            } else if (m.getInputMeta.getColumnType(colName).isnumeric && colData.toString.toLowerCase() == "true") {
              1.toString
            } else if (m.getInputMeta.getColumnType(colName).isnumeric && colData.toString.toLowerCase() == "false") {
              0.toString
            } else {
              colData.toString
            }
            rowBuilder.setValue(colName.toString, data)
        }

        builder.addRow(rowBuilder)
        val output = m.transform(builder.toMojoFrame)
        val predictions = output.getColumnNames.zipWithIndex.map { case (_, i) =>
          val predictedRows = output.getColumnData(i).asInstanceOf[Array[_]]
          if (predictedRows.length != 1) {
            throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
          }
          predictedRows(0).toString
        }

        Mojo2Prediction(predictions.toList)
    }

  def defaultFileName: String = H2OMOJOPipelineModel.defaultFileName

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  override def write: MLWriter = new H2OMOJOPipelineModelWriter(this)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val flatten = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val names = flatten.schema.fields.map(f => flatten(f.name))

    // get the altered frame
    flatten.select(col("*"), modelUdf(flatten.columns)(struct(names: _*)).as(outputCol))
  }

  def predictionSchema(): Seq[StructField] = {
    val fields = StructField("original", ArrayType(DoubleType)) :: Nil
    Seq(StructField(outputCol, StructType(fields), nullable = false))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema ++ predictionSchema())
  }
}

private[models] class H2OMOJOPipelineModelWriter(instance: H2OMOJOPipelineModel) extends MLWriter {

  @org.apache.spark.annotation.Since("1.6.0")
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val outputPath = new Path(path, instance.defaultFileName)
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val out = fs.create(qualifiedOutputPath)
    try {
      out.write(instance.mojoData)
    } finally {
      out.close()
    }
    logInfo(s"Saved to: $qualifiedOutputPath")
  }
}

private[models] class H2OMOJOModelPipelineReader
(val defaultFileName: String) extends MLReader[H2OMOJOPipelineModel] {

  private val className = implicitly[ClassTag[H2OMOJOPipelineModel]].runtimeClass.getName

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): H2OMOJOPipelineModel = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

    val inputPath = new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    val mojoData = Stream.continually(is.read()).takeWhile(_ != -1).map(_.toByte).toArray

    val h2oModel = make(mojoData, metadata.uid)(sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  def make(mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): H2OMOJOPipelineModel = {
    new H2OMOJOPipelineModel(mojoData, uid)
  }
}


object H2OMOJOPipelineModel extends MLReadable[H2OMOJOPipelineModel] {
  val defaultFileName = "mojo_pipeline_model"

  @Since("1.6.0")
  override def read: MLReader[H2OMOJOPipelineModel] = new H2OMOJOModelPipelineReader(defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OMOJOPipelineModel = super.load(path)

  def createFromMojo(path: String): H2OMOJOPipelineModel = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, new File(path).getName)
  }

  def createFromMojo(is: InputStream, uid: String = Identifiable.randomUID("mojoPipelineModel")): H2OMOJOPipelineModel = {
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    val sparkMojoModel = new H2OMOJOPipelineModel(mojoData, uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Pipeline Mojo
    sparkMojoModel
  }
}
