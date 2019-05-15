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
import ai.h2o.mojos.runtime.frame.MojoColumn.Type
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.internal.Logging
import org.apache.spark.ml.h2o.param.H2OMOJOPipelineModelParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._
import py_sparkling.ml.models.{H2OMOJOPipelineModel => PyH2OMOJOPipelineModel}
import water.util.DeprecatedMethod

import scala.collection.mutable
import scala.util.Random


class H2OMOJOPipelineModel(override val uid: String)
  extends SparkModel[H2OMOJOPipelineModel] with H2OMOJOPipelineModelParams with MLWritable with HasMojoData {

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
        val m = H2OMOJOModelCache.getOrCreateModel(uid, getMojoData)
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

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  private def selectFromArray(idx: Int) = udf[Double, mutable.WrappedArray[Double]] {
    pred => pred(idx)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val flattenedDF = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val relevantColumnNames = flattenedDF.columns.intersect(getFeaturesCols())
    val args = relevantColumnNames.map(flattenedDF(_))

    // get the altered frame
    val frameWithPredictions = flattenedDF.select(col("*"), modelUdf(relevantColumnNames)(struct(args: _*)).as(getPredictionCol))

    val fr = if (getNamedMojoOutputColumns()) {

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
      var frameWithExtractedPredictions: DataFrame = frameWithPredictions
      getOutputNames().indices.foreach { idx =>
        frameWithExtractedPredictions = frameWithExtractedPredictions.withColumn(tempColNames(idx),
          selectFromArray(idx)(frameWithExtractedPredictions.col(getPredictionCol + ".preds")))
      }

      // Transform the columns at the top level under "output" column
      val nestedPredictionCols = tempColNames.indices.map { idx => tempCols(idx).alias(getOutputNames()(idx)) }
      val frameWithNestedPredictions = frameWithExtractedPredictions.withColumn(getPredictionCol, struct(nestedPredictionCols: _*))

      // Remove the temporary columns at the top level and return
      val frameWithoutTempCols = frameWithNestedPredictions.drop(tempColNames: _*)

      frameWithoutTempCols
    } else {
      frameWithPredictions
    }

    H2OMOJOModelCache.removeModel(uid)

    fr
  }

  def predictionSchema(): Seq[StructField] = {
    val fields = StructField("original", ArrayType(DoubleType)) :: Nil
    Seq(StructField(getPredictionCol, StructType(fields), nullable = false))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema ++ predictionSchema())
  }

  @DeprecatedMethod("getFeaturesCols")
  def getInputNames(): Array[String] = getFeaturesCols()

  @DeprecatedMethod
  def getOutputNames(): Array[String] = H2OMOJOModelCache.getOrCreateModel(uid, getMojoData).getOutputMeta.getColumnNames

  def selectPredictionUDF(column: String) = {
    if (!getOutputNames().contains(column)) {
      throw new IllegalArgumentException(s"Column '$column' is not defined as the output column in MOJO Pipeline.")
    }
    if (getNamedMojoOutputColumns()) {
      val func = udf[Double, Double] {
        identity
      }
      func(col(s"$getPredictionCol.`$column`")).alias(column)
    } else {
      val func = selectFromArray(getOutputNames().indexOf(column))
      func(col(s"$getPredictionCol.preds")).alias(column)
    }
  }

  override def write: MLWriter = new H2OMOJOWriter(this, getMojoData)
}

object H2OMOJOPipelineModel extends H2OMOJOReadable[PyH2OMOJOPipelineModel] with H2OMOJOLoader[PyH2OMOJOPipelineModel] {

  override def createFromMojo(mojoData: Array[Byte], uid: String): PyH2OMOJOPipelineModel = {
    val model = new PyH2OMOJOPipelineModel(uid)
    val reader = MojoPipelineReaderBackendFactory.createReaderBackend(new ByteArrayInputStream(mojoData))
    val featureCols = MojoPipeline.loadFrom(reader).getInputMeta.getColumnNames
    model.set(model.featuresCols, featureCols)
    model.setMojoData(mojoData)
    model
  }
}


object H2OMOJOModelCache extends Logging {
  private var modelCache = scala.collection.mutable.Map[String, MojoPipeline]()

  def getOrCreateModel(uid: String, mojoData: Array[Byte]): MojoPipeline = synchronized {
    if (!modelCache.exists(_._1 == uid)) {
      logDebug("Creating new instance of MOJO model '" + uid + "'")
      val reader = MojoPipelineReaderBackendFactory.createReaderBackend(new ByteArrayInputStream(mojoData))
      modelCache += (uid -> MojoPipeline.loadFrom(reader))
    }
    modelCache(uid)
  }

  def removeModel(uid: String): Option[MojoPipeline] = synchronized {
    logDebug("Removing instance of MOJO model '" + uid + "' from model cache.")
    modelCache.remove(uid)
  }
}
