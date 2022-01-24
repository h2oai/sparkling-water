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
import ai.h2o.mojos.runtime.frame.MojoFrame
import ai.h2o.sparkling.ml.params.{H2OAlgorithmMOJOParams, H2OBaseMOJOParams, HasFeatureTypesOnMOJO}
import org.apache.spark.ml.param._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import ai.h2o.sparkling.sql.functions.{udf => swudf}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Model
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class H2OMOJOPipelineModel(override val uid: String)
  extends Model[H2OMOJOPipelineModel]
  with H2OMOJOFlattenedInput
  with HasMojo
  with H2OMOJOWritable
  with H2OAlgorithmMOJOParams
  with H2OBaseMOJOParams
  with HasFeatureTypesOnMOJO {

  H2OMOJOPipelineCache.startCleanupThread()

  // private parameter used to store MOJO output columns
  protected final val outputSubCols: StringArrayParam =
    new StringArrayParam(this, "outputSubCols", "Names of sub-columns under the output column")
  protected final val outputSubTypes: StringArrayParam =
    new StringArrayParam(this, "outputSubTypes", "Types of sub-columns under the output column")

  // MOJO output columns describing contributions
  protected final val outputSubColsContributions: StringArrayParam =
    new StringArrayParam(this, "outputSubColsContributions", "Names of contribution sub-columns under the output column")
  protected final val outputSubTypesContributions: StringArrayParam =
    new StringArrayParam(this, "outputSubTypesContributions", "Types of contribution sub-columns under the output column")

  def getOutputSubCols(): Array[String] = $ { outputSubCols }

  @transient private lazy val mojoPipeline: MojoPipeline = {
    H2OMOJOPipelineCache.getMojoBackend(uid, getMojo, this)
  }

  // As the mojoPipeline can't provide predictions and contributions at the same time, then
  // if contributions are requested, there is utilized a second pipeline setup the way to calculate contributions
  @transient private lazy val mojoPipelineContributions: MojoPipeline = {
    val pipeline = H2OMOJOPipelineCache.getMojoBackend(uid + ".contributions", getMojo, this)
    pipeline.setShapPredictContribOriginal(true)
    pipeline
  }


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

  private val modelUdf = (names: Array[String]) => {
    val schema = getPredictionColSchemaInternal()
    val function = (r: Row) => {
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

      def mojoFrameToArray(mf : MojoFrame) = {
        mf.getColumnNames.zipWithIndex.map {
          case (_, i) =>
            val columnData = mf.getColumnData(i).asInstanceOf[Array[_]]
            if (columnData.length != 1) {
              throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
            }
            columnData(0)
        }
      }

      builder.addRow(rowBuilder)
      val inputMojoFrame = builder.toMojoFrame

      val outputPredictions = mojoPipeline.transform(inputMojoFrame)
      val predictions = mojoFrameToArray(outputPredictions)

      val contributions = if (getWithContributions()) {
        val outputContributions = mojoPipelineContributions.transform(inputMojoFrame)
        mojoFrameToArray(outputContributions)
      } else {
        Array[Any]()
      }

      val content = if (getNamedMojoOutputColumns()) {
        predictions ++ contributions
      } else {
        Array[Any](predictions ++ contributions)
      }
      new GenericRowWithSchema(content, schema)
    }
    swudf(function, schema)
  }

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = applyPredictionUdf(dataset, modelUdf)

  private def toSparkType(t: Type): DataType = t match {
    case Type.Bool => BooleanType
    case Type.Float32 => FloatType
    case Type.Float64 => DoubleType
    case Type.Int32 => IntegerType
    case Type.Int64 => LongType
    case Type.Str => StringType
    case Type.Time64 => TimestampType
  }

  private def getPredictionColSchemaInternal(): StructType = {
    if (getNamedMojoOutputColumns()) {
      val outputPredictions = getOutputSubCols().zip($(outputSubTypes))
      val outputContributions = $(outputSubColsContributions).zip($(outputSubTypesContributions))
      val output = outputPredictions ++ outputContributions
      StructType(output.map { case (cn, ct) => StructField(cn, toSparkType(Type.valueOf(ct)), nullable = true) })
    } else {
      StructType(StructField("preds", ArrayType(DoubleType, containsNull = false), nullable = true) :: Nil)
    }
  }

  protected def getPredictionColSchema(): Seq[StructField] = {
    val predictionType = getPredictionColSchemaInternal()
    Seq(StructField(getPredictionCol(), predictionType, nullable = true))
  }

  def selectPredictionUDF(column: String): Column = {
    if (getNamedMojoOutputColumns()) {
      val func = udf[Double, Double] {
        identity
      }
      func(col(s"${getPredictionCol()}.`$column`")).alias(column)
    } else {
      val idx = getOutputSubCols().indexOf(column)
      col(s"${getPredictionCol()}.preds").getItem(idx).alias(column)
    }
  }

  override protected def inputColumnNames: Array[String] = getFeaturesCols()

  override protected def outputColumnName: String = getPredictionCol()

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(schema.fields ++ getPredictionColSchema())
  }
}

object H2OMOJOPipelineModel extends H2OMOJOReadable[H2OMOJOPipelineModel] with H2OMOJOLoader[H2OMOJOPipelineModel] {
  override def createFromMojo(mojo: InputStream, uid: String, settings: H2OMOJOSettings): H2OMOJOPipelineModel = {
    val model = new H2OMOJOPipelineModel(uid)
    model.setMojo(mojo, uid)

    setGeneralParameterrs(model, settings)
    setPredictionPipelineParameterrs(model)
    setContributionPipelineParameters(model, settings)

    model
  }

  private def setGeneralParameterrs(model: H2OMOJOPipelineModel, settings: H2OMOJOSettings): Any = {
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.withContributions, settings.withContributions)
  }

  private def setPredictionPipelineParameterrs(model: H2OMOJOPipelineModel) = {
    val pipelineMojo = MojoPipelineService.loadPipeline(model.getMojo())
    val inputCols = pipelineMojo.getInputMeta.getColumns.asScala
    val featureCols = inputCols.map(_.getColumnName).toArray
    model.set(model.featuresCols, featureCols)
    val featureTypeNames = inputCols.map(_.getColumnType.toString)
    val featureTypesMap = featureCols.zip(featureTypeNames).toMap
    val outputCols = pipelineMojo.getOutputMeta.getColumns.asScala
    model.set(model.featureTypes, featureTypesMap)
    model.set(model.outputSubCols, outputCols.map(_.getColumnName).toArray)
    model.set(model.outputSubTypes, outputCols.map(_.getColumnType.toString).toArray)
  }

  private def setContributionPipelineParameters(model: H2OMOJOPipelineModel, settings: H2OMOJOSettings): Any = {
    if (settings.withContributions) {
      val pipelineMojoContributions = MojoPipelineService.loadPipeline(model.getMojo())
      pipelineMojoContributions.setShapPredictContribOriginal(settings.withContributions)
      val outputColsContributions = pipelineMojoContributions.getOutputMeta.getColumns.asScala
      model.set(model.outputSubColsContributions, outputColsContributions.map(_.getColumnName).toArray)
      model.set(model.outputSubTypesContributions, outputColsContributions.map(_.getColumnType.toString).toArray)
    } else {
      model.set(model.outputSubColsContributions, Array[String]())
      model.set(model.outputSubTypesContributions, Array[String]())
    }
  }
}

private object H2OMOJOPipelineCache extends H2OMOJOBaseCache[MojoPipeline, H2OMOJOPipelineModel] {
  override def loadMojoBackend(mojo: File, model: H2OMOJOPipelineModel): MojoPipeline = {
    MojoPipelineService.loadPipeline(mojo)
  }
}
