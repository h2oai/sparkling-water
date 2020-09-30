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
import java.util.UUID

import ai.h2o.mojos.runtime.MojoPipeline
import ai.h2o.mojos.runtime.api.backend.{MemoryReaderBackend, ZipFileReaderBackend}
import ai.h2o.mojos.runtime.api.{MojoPipelineService, PipelinePreprocessor}
import ai.h2o.mojos.runtime.frame.MojoColumn.Type
import ai.h2o.mojos.runtime.frame.MojoFrameMeta
import ai.h2o.sparkling.ml.params.H2OMOJOPipelineParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.mutable

class H2OMOJOPipelineModel(override val uid: String)
  extends H2OMOJOModelBase[H2OMOJOPipelineModel]
  with H2OMOJOPipelineParams {

  H2OMOJOPipelineCache.startCleanupThread()

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

    val fr = if ($(namedMojoOutputColumns)) {
      val randName = if ($(expandNamedMojoOutputColumns)) $(predictionCol) else UUID.randomUUID().toString
      val predictionArray = s"${randName}.preds"
      val predictionCols = $(outputCols).zipWithIndex.map(i => enforceNullability(col(predictionArray)(i._2)).as(i._1))
      val expandedFrameWithPredictions =
        if ($(expandNamedMojoOutputColumns))
          frameWithPredictions
            .select(Seq(col("*")) ++ predictionCols: _*)
            .drop($(predictionCol))
        else
          frameWithPredictions
            .withColumnRenamed($(predictionCol), randName)
            .withColumn($(predictionCol), enforceNullability(struct(predictionCols: _*)))
            .drop(randName)

      expandedFrameWithPredictions
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
    if (getNamedMojoOutputColumns() && getExpandNamedMojoOutputColumns())
      predictionType.fields
    else
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
    val backend = ZipFileReaderBackend.open(model.getMojo())
    val pipeline = if (settings.removeModel) {
      val factory = MojoPipelineService.INSTANCE.get(backend)
      val loader = factory.createLoader(backend, null)
      loader.preprocessAndLoad(PipelinePreprocessor.EXTRACT_MODEL).head
    } else {
      MojoPipelineService.loadPipeline(model.getMojo())
    }
    // Configure model
    model.set(model.featuresCols, pipeline.getInputMeta.getColumnNames)
    model.set(model.outputCols, pipeline.getOutputMeta.getColumnNames)
    model.set(model.mojoInputSchema, toSchema(pipeline.getInputMeta))
    model.set(model.mojoOutputSchema, toSchema(pipeline.getOutputMeta))
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.removeModel -> settings.removeModel)
    model.set(model.expandNamedMojoOutputColumns -> settings.expandNamedMojoOutputColumns)
    model
  }

  def toSchema(f: MojoFrameMeta): StructType = {
    StructType(f.getColumnNames.zip(f.getColumnTypes).map(p => StructField(p._1, toSparkType(p._2), true)))
  }

  def toSparkType(t: Type): DataType = {
    t match {
      case Type.Bool => BooleanType
      case Type.Float32 => FloatType
      case Type.Float64 => DoubleType
      case Type.Int32 => IntegerType
      case Type.Int64 => LongType
      case Type.Str => StringType
      case Type.Time64 => TimestampType
    }
  }
}

private object H2OMOJOPipelineCache extends H2OMOJOBaseCache[MojoPipeline, H2OMOJOPipelineModel] {
  override def loadMojoBackend(mojo: File, model: H2OMOJOPipelineModel): MojoPipeline = {
    val backend = ZipFileReaderBackend.open(mojo.getCanonicalFile)
    if (model.getRemoveModel()) {
      val factory = MojoPipelineService.INSTANCE.get(backend)
      val loader = factory.createLoader(backend, null)
      loader.preprocessAndLoad(PipelinePreprocessor.EXTRACT_MODEL).head
    } else {
      MojoPipelineService.loadPipeline(backend)
    }
  }
}
