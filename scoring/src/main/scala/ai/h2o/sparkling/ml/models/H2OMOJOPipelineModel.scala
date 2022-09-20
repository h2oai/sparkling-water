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
import ai.h2o.mojos.runtime.api.{MojoPipelineService, PipelineConfig}
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
import scala.collection.mutable

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
    new StringArrayParam(
      this,
      "outputSubColsContributions",
      "Names of contribution sub-columns under the contributions column (contributions on original features)")
  protected final val outputSubTypesContributions: StringArrayParam =
    new StringArrayParam(
      this,
      "outputSubTypesContributions",
      "Types of contribution sub-columns under the contributions column (contributions on original features)")

  // MOJO output columns describing contributions on transformed features
  protected final val outputSubColsInternalContributions: StringArrayParam =
    new StringArrayParam(
      this,
      "outputSubColsInternalContributions",
      "Names of contribution sub-columns under the internal contributions column (contributions on transformed features)")
  protected final val outputSubTypesInternalContributions: StringArrayParam =
    new StringArrayParam(
      this,
      "outputSubTypesInternalContributions",
      "Types of contribution sub-columns under the internal contributions column (contributions on transformed features)")

  def getOutputSubCols(): Array[String] = $ { outputSubCols }

  def getContributionsCol(): String = "contributions"

  def getInternalContributionsCol(): String = "internal_contributions"

  def getTransportCol(): String = "SparklingWater_transport"

  @transient private lazy val mojoPipeline: MojoPipeline = H2OMOJOPipelineCache.getMojoBackend(uid, getMojo)

  // As the mojoPipeline can't provide predictions and contributions at the same time, then
  // if contributions are requested, there is utilized a second pipeline
  // that's setup the way to calculate contributions.
  @transient private lazy val mojoPipelineContributions: MojoPipeline = {
    H2OMOJOPipelineCache.getMojoBackend(uid + ".contributions", getMojo, Map[String, Any]("enableShapOriginal" -> true))
  }

  // As the mojoPipeline can't provide predictions and contributions at the same time, then
  // if contributions are requested, there is utilized a second pipeline
  // that's setup the way to calculate contributions.
  @transient private lazy val mojoPipelineInternalContributions: MojoPipeline = {
    H2OMOJOPipelineCache.getMojoBackend(
      uid + ".internal_contributions",
      getMojo,
      Map[String, Any]("enableShap" -> true))
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

  private def rowToMojoFrame(mojoPipeline: MojoPipeline, names: Array[String], row: Row): MojoFrame = {
    val builder = mojoPipeline.getInputFrameBuilder
    val rowBuilder = builder.getMojoRowBuilder
    val filtered = row.getValuesMap[Any](names).filter { case (n, _) => mojoPipeline.getInputMeta.contains(n) }

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
    builder.toMojoFrame
  }

  private val modelUdf = (names: Array[String]) => {
    val schemaTransport = getTransportSchema()
    val schemaPredict = getPredictionColSchemaInternal()
    val namedOutputColumns = getNamedMojoOutputColumns()

    val function = (row: Row) => {
      val contentBuilder = mutable.ArrayBuffer[Any]()

      val inputMojoFrameForPredictions = rowToMojoFrame(mojoPipeline, names, row)
      val outputPredictions = mojoPipeline.transform(inputMojoFrameForPredictions)
      val predictions = mojoFrameToArray(outputPredictions)
      val content = if (namedOutputColumns) predictions else Array[Any](predictions)
      contentBuilder += new GenericRowWithSchema(content, schemaPredict)

      if (getWithContributions()) {
        val columnNames = $(outputSubColsContributions)
        val types = $(outputSubTypesContributions)
        val schema = getContributionsColSchemaInternal(columnNames, types)
        addContributions(mojoPipelineContributions, names, row, schema, contentBuilder)
      }

      if (getWithInternalContributions()) {
        val columnNames = $(outputSubColsInternalContributions)
        val types = $(outputSubTypesInternalContributions)
        val schema = getContributionsColSchemaInternal(columnNames, types)
        addContributions(mojoPipelineInternalContributions, names, row, schema, contentBuilder)
      }

      new GenericRowWithSchema(contentBuilder.toArray, schemaTransport)
    }

    swudf(function, schemaTransport)
  }

  private def addContributions(
      pipeline: MojoPipeline,
      names: Array[String],
      row: Row,
      schema: StructType,
      contentBuilder: mutable.ArrayBuffer[Any]): Unit = {
    val inputMojoFrameForContributions = rowToMojoFrame(pipeline, names, row)
    val outputContributions = pipeline.transform(inputMojoFrameForContributions)
    val contributions = mojoFrameToArray(outputContributions)
    contentBuilder += new GenericRowWithSchema(contributions, schema)
  }

  private def mojoFrameToArray(mf: MojoFrame) = {
    mf.getColumnNames.zipWithIndex.map {
      case (_, i) =>
        val columnData = mf.getColumnData(i).asInstanceOf[Array[_]]
        if (columnData.length != 1) {
          throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
        }
        columnData(0)
    }
  }

  override def copy(extra: ParamMap): H2OMOJOPipelineModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val baseDf = applyPredictionUdf(dataset, modelUdf)
    val withPredictionsDF = baseDf.withColumn(getPredictionCol(), col(s"${getTransportCol()}.${getPredictionCol()}"))
    val withContributionsDF = if (getWithContributions()) {
      withPredictionsDF.withColumn(getContributionsCol(), col(s"${getTransportCol()}.${getContributionsCol()}"))
    } else {
      withPredictionsDF
    }
    val withInternalContributionsDF = if (getWithInternalContributions()) {
      withContributionsDF.withColumn(
        getInternalContributionsCol(),
        col(s"${getTransportCol()}.${getInternalContributionsCol()}"))
    } else {
      withContributionsDF
    }
    withInternalContributionsDF.drop(getTransportCol())
  }

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
      StructType(outputPredictions.map {
        case (cn, ct) => StructField(cn, toSparkType(Type.valueOf(ct)), nullable = true)
      })
    } else {
      StructType(StructField("preds", ArrayType(DoubleType, containsNull = false), nullable = true) :: Nil)
    }
  }

  protected def getPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getPredictionCol(), getPredictionColSchemaInternal(), nullable = true))
  }

  private def getContributionsColSchemaInternal(names: Array[String], types: Array[String]): StructType = {
    StructType(names.zip(types).map {
      case (cn, ct) => StructField(cn, toSparkType(Type.valueOf(ct)), nullable = true)
    })
  }

  protected def getContributionsColSchema(): Seq[StructField] = {
    if (getWithContributions()) {
      val names = $(outputSubColsContributions)
      val types = $(outputSubTypesContributions)
      Seq(StructField(getContributionsCol(), getContributionsColSchemaInternal(names, types), nullable = true))
    } else {
      Seq.empty[StructField]
    }
  }

  protected def getInternalContributionsColSchema(): Seq[StructField] = {
    if (getWithInternalContributions()) {
      val names = $(outputSubColsInternalContributions)
      val types = $(outputSubTypesInternalContributions)
      Seq(StructField(getInternalContributionsCol(), getContributionsColSchemaInternal(names, types), nullable = true))
    } else {
      Seq.empty[StructField]
    }
  }

  private def getTransportSchema() = {
    StructType(getPredictionColSchema() ++ getContributionsColSchema() ++ getInternalContributionsColSchema())
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

  override protected def outputColumnName: String = getTransportCol()

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    StructType(
      schema.fields ++ getPredictionColSchema() ++ getContributionsColSchema() ++ getInternalContributionsColSchema())
  }
}

object H2OMOJOPipelineModel extends H2OMOJOReadable[H2OMOJOPipelineModel] with H2OMOJOLoader[H2OMOJOPipelineModel] {
  override def createFromMojo(mojo: InputStream, uid: String, settings: H2OMOJOSettings): H2OMOJOPipelineModel = {
    val model = new H2OMOJOPipelineModel(uid)
    model.setMojo(mojo, uid)

    setGeneralParameterrs(model, settings)
    setPredictionPipelineParameterrs(model)
    setContributionPipelineParameters(model, settings)
    setInternalContributionPipelineParameters(model, settings)

    model
  }

  private def setGeneralParameterrs(model: H2OMOJOPipelineModel, settings: H2OMOJOSettings): Any = {
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.withContributions, settings.withContributions)
    model.set(model.withInternalContributions, settings.withInternalContributions)
  }

  private def setPredictionPipelineParameterrs(model: H2OMOJOPipelineModel) = {
    val pipelineMojo = MojoPipelineService.loadPipeline(model.getMojo(), PipelineConfig.DEFAULT)
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
      val config = PipelineConfig.builder().enableShapOriginal(true)
      val pipelineMojoContributions = MojoPipelineService.loadPipeline(model.getMojo(), config)
      val outputColsContributions = pipelineMojoContributions.getOutputMeta.getColumns.asScala
      model.set(model.outputSubColsContributions, outputColsContributions.map(_.getColumnName).toArray)
      model.set(model.outputSubTypesContributions, outputColsContributions.map(_.getColumnType.toString).toArray)
    } else {
      model.set(model.outputSubColsContributions, Array[String]())
      model.set(model.outputSubTypesContributions, Array[String]())
    }
  }

  private def setInternalContributionPipelineParameters(model: H2OMOJOPipelineModel, settings: H2OMOJOSettings): Any = {
    if (settings.withInternalContributions) {
      val config = PipelineConfig.builder().enableShap(true)
      val pipelineMojoInternalContributions = MojoPipelineService.loadPipeline(model.getMojo(), config)
      val outputColsContributions = pipelineMojoInternalContributions.getOutputMeta.getColumns.asScala
      model.set(model.outputSubColsInternalContributions, outputColsContributions.map(_.getColumnName).toArray)
      model.set(
        model.outputSubTypesInternalContributions,
        outputColsContributions.map(_.getColumnType.toString).toArray)
    } else {
      model.set(model.outputSubColsInternalContributions, Array[String]())
      model.set(model.outputSubTypesInternalContributions, Array[String]())
    }
  }
}

private object H2OMOJOPipelineCache extends H2OMOJOBaseCache[MojoPipeline] {
  override def loadMojoBackend(mojo: File, configMap: Map[String, Any]): MojoPipeline = {
    val configBuilder = PipelineConfig.builder()
    if (configMap.contains("enableShapOriginal") && configMap("enableShapOriginal").asInstanceOf[Boolean]) {
      configBuilder.enableShapOriginal(true)
    }
    if (configMap.contains("enableShap") && configMap("enableShap").asInstanceOf[Boolean]) {
      configBuilder.enableShap(true)
    }
    val config = configBuilder.build()
    MojoPipelineService.loadPipeline(mojo, config)
  }
}
