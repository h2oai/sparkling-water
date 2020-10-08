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

import java.io._

import ai.h2o.mojos.runtime.MojoPipeline
import ai.h2o.mojos.runtime.api.MojoPipelineService
import ai.h2o.mojos.runtime.frame.MojoColumn.Type
import org.apache.spark.ml.param.{ParamMap, StringArrayParam}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random
import scala.collection.JavaConverters._

class H2OMOJOPipelineModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOPipelineModel] {

  H2OMOJOPipelineCache.startCleanupThread()
  // private parameter used to store MOJO output columns
  protected final val outputCols: StringArrayParam = new StringArrayParam(this, "outputCols", "OutputCols")

  @transient private lazy val mojoPipeline: MojoPipeline = {
    H2OMOJOPipelineCache.getMojoBackend(uid, getMojo, this)
  }

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
    } else if (colType.isAssignableFrom(classOf[String]) && !colData.isInstanceOf[String]) {
      // MOJO expects String, but we have DataFrame with different column type, cast to String
      colData.toString
    } else {
      colData
    }
  }

  private val modelUdf = (names: Array[String]) =>
    udf[Mojo2Prediction, Row] { r: Row =>
      val builder = mojoPipeline.getInputFrameBuilder
      val rowBuilder = builder.getMojoRowBuilder
      val filtered = r.getValuesMap[Any](names).filter { case (n, _) => mojoPipeline.getInputMeta.contains(n) }

      filtered.foreach {
        case (colName, colData) =>
          val prepared = prepareBooleans(mojoPipeline.getInputMeta.getColumnType(colName), colData)

          prepared match {
            case v: Boolean => rowBuilder.setBool(colName, v)
            case v: Char => rowBuilder.setChar(colName, v)
            case v: Byte => rowBuilder.setByte(colName, v)
            case v: Short => rowBuilder.setShort(colName, v)
            case v: Int => rowBuilder.setInt(colName, v)
            case v: Long => rowBuilder.setLong(colName, v)
            case v: Float => rowBuilder.setFloat(colName, v)
            case v: Double => rowBuilder.setDouble(colName, v)
            case v: String =>
              if (mojoPipeline.getInputMeta.getColumnType(colName).isAssignableFrom(classOf[String])) {
                // if String is expected, no need to do the parse
                rowBuilder.setString(colName, v)
              } else {
                // some other type is expected, we need to perform the parse
                rowBuilder.setValue(colName, v)
              }
            case v: java.sql.Timestamp =>
              if (mojoPipeline.getInputMeta.getColumnType(colName).isAssignableFrom(classOf[java.sql.Timestamp])) {
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
      val output = mojoPipeline.transform(builder.toMojoFrame)
      val predictions = output.getColumnNames.zipWithIndex.map {
        case (_, i) =>
          val columnOutput = output.getColumnData(i)
          val predictedVal = columnOutput match {
            case floats: Array[Float] =>
              floats.map(_.asInstanceOf[Double])
            case _ =>
              columnOutput.asInstanceOf[Array[Double]]
          }
          if (predictedVal.length != 1) {
            throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
          }
          predictedVal(0)
      }

      Mojo2Prediction(predictions.toList)
    }

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  private def selectFromArray(idx: Int) = udf[Double, mutable.WrappedArray[Double]] { pred =>
    pred(idx)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val frameWithPredictions = applyPredictionUdf(dataset, modelUdf)

    val fr = if (getNamedMojoOutputColumns()) {

      def uniqueRandomName(colName: String, r: Random) = {
        var randName = r.nextString(30)
        while (colName == randName) {
          randName = r.nextString(30)
        }
        randName
      }

      val r = new scala.util.Random(31)
      val tempColNames = $(outputCols).map(uniqueRandomName(_, r))
      val tempCols = tempColNames.map(col)

      // Transform the resulted Array of predictions into own but temporary columns
      // Temporary columns are created as we can't create the columns directly as nested ones
      val predictionCols = $(outputCols).indices.map { idx =>
        selectFromArray(idx)(frameWithPredictions.col(s"${getPredictionCol()}.preds"))
      }

      val frameWithExtractedPredictions = $(outputCols).indices.foldLeft(frameWithPredictions) {
        case (df, idx) =>
          df.withColumn(tempColNames(idx), enforceNullability(predictionCols(idx)))
      }

      // Transform the columns at the top level under "output" column
      val nestedPredictionCols = tempColNames.indices.map { idx =>
        tempCols(idx).alias($(outputCols)(idx))
      }
      val resultCol = struct(nestedPredictionCols: _*)
      val nullableResultCol = enforceNullability(resultCol)
      val frameWithNestedPredictions = frameWithExtractedPredictions.withColumn(getPredictionCol(), nullableResultCol)

      // Remove the temporary columns at the top level and return
      val frameWithoutTempCols = frameWithNestedPredictions.drop(tempColNames: _*)

      frameWithoutTempCols
    } else {
      frameWithPredictions
    }
    fr
  }

  private def enforceNullability(column: Column): Column = {
    when(column.isNull, null).otherwise(column)
  }

  override protected def getPredictionColSchema(): Seq[StructField] = {
    val predictionType = if (getNamedMojoOutputColumns()) {
      StructType($(outputCols).map(oc => StructField(oc, DoubleType, nullable = true)))
    } else {
      StructType(StructField("preds", ArrayType(DoubleType, containsNull = false), nullable = true) :: Nil)
    }
    Seq(StructField(getPredictionCol(), predictionType, nullable = true))
  }

  def selectPredictionUDF(column: String): Column = {
    if (getNamedMojoOutputColumns()) {
      val func = udf[Double, Double] {
        identity
      }
      func(col(s"${getPredictionCol()}.`$column`")).alias(column)
    } else {
      val func = selectFromArray($(outputCols).indexOf(column))
      func(col(s"${getPredictionCol()}.preds")).alias(column)
    }
  }
}

object H2OMOJOPipelineModel extends H2OMOJOReadable[H2OMOJOPipelineModel] with H2OMOJOLoader[H2OMOJOPipelineModel] {
  override def createFromMojo(mojo: InputStream, uid: String, settings: H2OMOJOSettings): H2OMOJOPipelineModel = {
    val model = new H2OMOJOPipelineModel(uid)
    model.setMojo(mojo, uid)
    val pipelineMojo = MojoPipelineService.loadPipeline(model.getMojo())
    val featureCols = pipelineMojo.getInputMeta.getColumnNames
    model.set(model.featuresCols, featureCols)
    val featureTypeNames = pipelineMojo.getInputMeta.getColumnTypes().map(_.toString)
    val featureTypesMap = pipelineMojo.getInputMeta.getColumnNames.zip(featureTypeNames).toMap
    model.set(model.featureTypes, featureTypesMap)
    model.set(model.outputCols, pipelineMojo.getOutputMeta.getColumnNames)
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model
  }
}

private object H2OMOJOPipelineCache extends H2OMOJOBaseCache[MojoPipeline, H2OMOJOPipelineModel] {
  override def loadMojoBackend(mojo: File, model: H2OMOJOPipelineModel): MojoPipeline = {
    MojoPipelineService.loadPipeline(mojo)
  }
}
