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
import ai.h2o.mojos.runtime.frame.MojoColumn.Type
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.h2o.param.H2OMOJOPipelineModelParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.reflect.{ClassTag, classTag}
import scala.util.Random


class H2OMOJOPipelineModel(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMOJOPipelineModel] with H2OMOJOPipelineModelParams with MLWritable {

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

  case class Mojo2Prediction(preds: List[Double])

  private def prepareBooleans(colType: Type, colData: Any): Any = {
    if (colData == null) {
      null
    } else if (colType == Type.Bool) {
      // expected is Bool, do nothing
      colData
    } else if (colType != Type.Bool && colType.isnumeric && colData.toString.toLowerCase() == "true") {
      // expected is Numeric value, convert to number
      1
    } else if (colType != Type.Bool && colType.isnumeric && colData.toString.toLowerCase() == "false") {
      0
    } else {
      colData
    }

  }

  private val modelUdf = (names: Array[String]) =>
    udf[Mojo2Prediction, Row] {
      r: Row =>
        val m = getOrCreateModel()
        val builder = m.getInputFrameBuilder
        val rowBuilder = builder.getMojoRowBuilder
        val filtered = r.getValuesMap[Any](names).filter { case (n, _) => m.getInputMeta.contains(n) }

        filtered.foreach {
          case (colName, colData) =>
            val prepared = prepareBooleans(m.getInputMeta.getColumnType(colName), colData)

            prepared match {
              case v: Boolean => rowBuilder.setBool(colName, v)
              case v: Char => rowBuilder.setChar(colName, v)
              case v: Byte => rowBuilder.setByte(colName, v)
              case v: Short => rowBuilder.setShort(colName, v)
              case v: Int => rowBuilder.setInt(colName, v)
              case v: Long => rowBuilder.setLong(colName, v)
              case v: Float => rowBuilder.setFloat(colName, v)
              case v: Double => rowBuilder.setDouble(colName, v)
              case v: String => if (m.getInputMeta.getColumnType(colName).isAssignableFrom(classOf[String])) {
                // if String is expected, no need to do the parse
                rowBuilder.setString(colName, v)
              } else {
                // some other type is expected, we need to perform the parse
                rowBuilder.setValue(colName, v)
              }
              case v: java.sql.Timestamp => if (m.getInputMeta.getColumnType(colName).isAssignableFrom(classOf[java.sql.Timestamp])) {
                rowBuilder.setTimestamp(colName, v)
              } else {
                // parse
                rowBuilder.setValue(colName, v.toString)
              }

              case v: java.sql.Date => rowBuilder.setDate(colName, v)
              case null => rowBuilder.setValue(colName, null)
              case v: Any =>
                // Some other type, do the parse
                rowBuilder.setValue(colName, v.toString)
            }
        }

        builder.addRow(rowBuilder)
        val output = m.transform(builder.toMojoFrame)
        val predictions = output.getColumnNames.zipWithIndex.map { case (_, i) =>
          val predictedVal = output.getColumnData(i).asInstanceOf[Array[Double]]
          if (predictedVal.length != 1) {
            throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
          }
          predictedVal(0)
        }

        Mojo2Prediction(predictions.toList)
    }

  def defaultFileName: String = H2OMOJOPipelineModel.defaultFileName

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  override def write: MLWriter = new H2OMOJOPipelineModelWriter(this)

  private def selectFromArray(idx: Int) = udf[Double, mutable.WrappedArray[Double]] {
    pred => pred(idx)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val flatten = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val names = flatten.schema.fields.map(f => flatten(f.name))

    // get the altered frame
    val frameWithPredictions = flatten.select(col("*"), modelUdf(flatten.columns)(struct(names: _*)).as(outputCol))

    // This behaviour is turned on by default since Sparkling Water 2.4.x, it can be disabled manually.
    if ($(namedMojoOutputColumns)) {

      def uniqueRandomName(colName: String, r: Random) = {
        var randName = r.nextString(30)
        while (colName == randName) {
          randName = r.nextString(30)
        }
        randName
      }

      val r = new scala.util.Random(31)
      val tempColNames = getOutputNames().map(uniqueRandomName(_, r))
      val tempCols = tempColNames.map(col)


      // Transform the resulted Array of predictions into own but temporary columns
      // Temporary columns are created as we can't create the columns directly as nested ones
      val predictionCols = getOutputNames().indices.map(idx => selectFromArray(idx)(frameWithPredictions.col(outputCol + ".preds")))
      val frameWithExtractedPredictions = frameWithPredictions.withColumns(tempColNames, predictionCols)

      // Transform the columns at the top level under "output" column
      val nestedPredictionCols = tempColNames.indices.map { idx => tempCols(idx).alias(getOutputNames()(idx)) }
      val frameWithNestedPredictions = frameWithExtractedPredictions.withColumn(outputCol, struct(nestedPredictionCols: _*))

      // Remove the temporary columns at the top level and return
      val frameWithoutTempCols = frameWithNestedPredictions.drop(tempColNames: _*)

      frameWithoutTempCols
    } else {
      logWarning(
        """
          | You are using Mojo Pipeline with the old-style output without properly named output columns.
          | This behaviour is now default, however since the next major release of Sparkling Water, the default will
          | be set to the variant currently enabled by calling mojo.set_named_mojo_output_columns(True) in PySparkling
          | and mojo.setNamedMojoOutputColumns(true) in Sparkling Water. This means that the output columns are added
          | separately and are named correctly.
          |
        """.stripMargin)

      frameWithPredictions
    }
  }

  def predictionSchema(): Seq[StructField] = {
    val fields = StructField("original", ArrayType(DoubleType)) :: Nil
    Seq(StructField(outputCol, StructType(fields), nullable = false))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema ++ predictionSchema())
  }

  def getInputNames(): Array[String] = getOrCreateModel().getInputMeta.getColumnNames

  def getInputTypes(): Array[MojoColumn.Type] = {
    getOrCreateModel().getInputMeta.getColumnTypes
  }

  def getOutputNames(): Array[String] = getOrCreateModel().getOutputMeta.getColumnNames

  def getOutputTypes(): Array[MojoColumn.Type] = {
    getOrCreateModel().getOutputMeta.getColumnTypes
  }

  def selectPredictionUDF(column: String) = {
    if (!getOutputNames().contains(column)) {
      throw new IllegalArgumentException(s"Column '$column' is not defined as the output column in MOJO Pipeline.")
    }
    if ($(namedMojoOutputColumns)) {
      val func = udf[Double, Double] {
        identity
      }
      func(col(s"$outputCol.`$column`")).alias(column)
    } else {
      val func = selectFromArray(getOutputNames().indexOf(column))
      func(col(s"$outputCol.preds")).alias(column)
    }
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

private[models] class H2OMOJOPipelineModelReader[T <: H2OMOJOPipelineModel : ClassTag]
(val defaultFileName: String) extends MLReader[T] {

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)

    val inputPath = new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    val mojoData = Stream.continually(is.read()).takeWhile(_ != -1).map(_.toByte).toArray

    val h2oModel = make(mojoData, metadata.uid)(sqlContext)
    metadata.getAndSetParams(h2oModel)
    h2oModel
  }

  def make(mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): T = {
    classTag[T].runtimeClass.getConstructor(classOf[Array[Byte]], classOf[String]).
      newInstance(mojoData, uid).asInstanceOf[T]
  }
}


class H2OMOJOPipelineModelHelper[T<: py_sparkling.ml.models.H2OMOJOPipelineModel](implicit m: ClassTag[T]) extends MLReadable[T]{
  val defaultFileName = "mojo_pipeline_model"

  @Since("1.6.0")
  override def read: MLReader[T] = new H2OMOJOPipelineModelReader[T](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): T = super.load(path)

  def createFromMojo(path: String): T = {
    val inputPath = new Path(path)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, new File(path).getName)
  }

  def createFromMojo(is: InputStream, uid: String = Identifiable.randomUID("mojoPipelineModel")): T = {
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Pipeline Mojo
    val sparkMojoModel = m.runtimeClass.getConstructor(classOf[Array[Byte]], classOf[String]).
      newInstance(mojoData, uid).asInstanceOf[T]
    sparkMojoModel
  }
}

object H2OMOJOPipelineModel extends H2OMOJOPipelineModelHelper[py_sparkling.ml.models.H2OMOJOPipelineModel] {}
