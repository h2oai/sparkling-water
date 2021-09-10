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

import java.io.{File, InputStream}

import _root_.hex.genmodel.attributes.ModelJsonReader
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import _root_.hex.genmodel.{MojoModel, MojoReaderBackendFactory}
import ai.h2o.sparkling.ml.internals.{H2OMetric, H2OModelCategory}
import ai.h2o.sparkling.ml.params._
import ai.h2o.sparkling.ml.utils.Utils
import ai.h2o.sparkling.utils.SparkSessionUtils
import com.google.gson._
import hex.ModelCategory
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import ai.h2o.sparkling.macros.DeprecatedMethod
import _root_.hex.genmodel.attributes.Table.ColumnType
import ai.h2o.sparkling.ml.metrics.H2OMetrics
import org.apache.spark.expose.Logging
import org.apache.spark.ml.Model
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

abstract class H2OMOJOModel
  extends Model[H2OMOJOModel]
  with H2OMOJOFlattenedInput
  with HasMojo
  with H2OMOJOWritable
  with H2OMOJOModelUtils
  with SpecificMOJOParameters
  with H2OBaseMOJOParams
  with HasFeatureTypesOnMOJO
  with Logging {

  H2OMOJOCache.startCleanupThread()
  protected final val modelDetails: NullableStringParam =
    new NullableStringParam(this, "modelDetails", "Raw details of this model.")
  protected final val trainingMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "trainingMetrics", "Training metrics represented as a map.")
  protected final val trainingMetricsObject: NullableMetricsParam =
    new NullableMetricsParam(this, "trainingMetricsObject", "Training metrics in strongly typed object.")
  protected final val validationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "validationMetrics", "Validation metrics represented as a map.")
  protected final val validationMetricsObject: NullableMetricsParam =
    new NullableMetricsParam(this, "validationMetricsObject", "Validation metrics in strongly typed object.")
  protected final val crossValidationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "crossValidationMetrics", "Cross Validation metrics represented as a map.")
  protected final val crossValidationMetricsObject: NullableMetricsParam =
    new NullableMetricsParam(this, "crossValidationMetricsObject", "Cross Validation metrics in strongly typed object.")
  protected final val trainingParams: MapStringStringParam =
    new MapStringStringParam(this, "trainingParams", "Training params")
  protected final val modelCategory: NullableStringParam =
    new NullableStringParam(this, "modelCategory", "H2O's model category")
  protected final val scoringHistory: NullableDataFrameParam =
    new NullableDataFrameParam(this, "scoringHistory", "Scoring history acquired during the model training.")
  protected final val featureImportances: NullableDataFrameParam =
    new NullableDataFrameParam(this, "featureImportances", "Feature imporanteces.")

  private[sparkling] final val numberOfCrossValidationModels: IntParam =
    new IntParam(this, "numberOfCrossValidationModels", "Number of cross validation models.")
  protected var crossValidationModels: Array[H2OMOJOModel] = null

  setDefault(
    modelDetails -> null,
    trainingMetrics -> Map.empty[String, Double],
    trainingMetricsObject -> null,
    validationMetrics -> Map.empty[String, Double],
    validationMetricsObject -> null,
    crossValidationMetrics -> Map.empty[String, Double],
    crossValidationMetricsObject -> null,
    trainingParams -> Map.empty[String, String],
    modelCategory -> null,
    scoringHistory -> null,
    featureImportances -> null,
    numberOfCrossValidationModels -> 0)

  /**
    * Returns a map of all metrics of the Double type calculated on the training dataset.
    */
  def getTrainingMetrics(): Map[String, Double] = $(trainingMetrics)

  /**
    * Returns an object holding all metrics of the Double type and also more complex performance information
    * calculated on the training dataset.
    */
  def getTrainingMetricsObject(): H2OMetrics = $(trainingMetricsObject)

  /**
    * Returns a map of all metrics of the Double type calculated on the validation dataset.
    */
  def getValidationMetrics(): Map[String, Double] = $(validationMetrics)

  /**
    * Returns an object holding all metrics of the Double type and also more complex performance information
    * calculated on the validation dataset.
    */
  def getValidationMetricsObject(): H2OMetrics = $(validationMetricsObject)

  /**
    * Returns a map of all combined cross-validation holdout metrics of the Double Type.
    */
  def getCrossValidationMetrics(): Map[String, Double] = $(crossValidationMetrics)

  /**
    * Returns an object holding all metrics of the Double type and also more complex performance information
    * combined from cross-validation holdouts.
    */
  def getCrossValidationMetricsObject(): H2OMetrics = $(crossValidationMetricsObject)

  /**
    * Returns a map of all metrics of the Double type. If the nfolds parameter was set, the metrics were combined from
    * cross-validation holdouts. If cross validations wasn't enabled, the metrics were calculated from a validation
    * dataset. If the validation dataset wasn't available, the metrics were calculated from the training dataset.
    */
  def getCurrentMetrics(): Map[String, Double] = {
    val nfolds = $(trainingParams).get("nfolds")
    val validationFrame = $(trainingParams).get("validation_frame")
    if (nfolds.isDefined && nfolds.get.toInt > 1) {
      getCrossValidationMetrics()
    } else if (validationFrame.isDefined) {
      getValidationMetrics()
    } else {
      getTrainingMetrics()
    }
  }

  /**
    * Returns a map of all metrics of the Double type. If the nfolds parameter was set, the object was combined from
    * cross-validation holdouts. If cross validations wasn't enabled, the object was calculated from a validation
    * dataset. If the validation dataset wasn't available, the object was calculated from the training dataset.
    */
  def getCurrentMetricsObject(): H2OMetrics = {
    val nfolds = $(trainingParams).get("nfolds")
    val validationFrame = $(trainingParams).get("validation_frame")
    if (nfolds.isDefined && nfolds.get.toInt > 1) {
      getCrossValidationMetricsObject()
    } else if (validationFrame.isDefined) {
      getValidationMetricsObject()
    } else {
      getTrainingMetricsObject()
    }
  }

  def getTrainingParams(): Map[String, String] = $(trainingParams)

  def getModelCategory(): String = $(modelCategory)

  def getModelDetails(): String = $(modelDetails)

  def getDomainValues(): Map[String, Array[String]] = {
    val mojo = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    val columns = mojo.getNames
    columns.map(col => col -> mojo.getDomainValues(col)).toMap
  }

  def getScoringHistory(): DataFrame = $(scoringHistory)

  def getFeatureImportances(): DataFrame = $(featureImportances)

  def getCrossValidationModels(): Seq[this.type] = {
    if (crossValidationModels == null) {
      null
    } else {
      val result = new Array[this.type](crossValidationModels.length)
      for (i <- 0 until crossValidationModels.length) {
        result(i) = crossValidationModels(i).asInstanceOf[this.type]
      }
      result
    }
  }

  private[sparkling] def getCrossValidationModelsAsArray(): Array[H2OMOJOModel] = crossValidationModels

  private[sparkling] def setCrossValidationModels(models: Array[H2OMOJOModel]): this.type = {
    crossValidationModels = models
    if (models != null) {
      set(numberOfCrossValidationModels, models.length)
    }
    this
  }

  /**
    * The method returns an internal H2O-3 mojo model, which can be subsequently used with
    * [[EasyPredictModelWrapper]] to perform predictions on individual rows.
    */
  def unwrapMojoModel(): _root_.hex.genmodel.MojoModel = H2OMOJOCache.getMojoBackend(uid, getMojo, this)

  protected override def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = getRelevantColumnNames(flatDataFrame, inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial | ModelCategory.Regression | ModelCategory.Multinomial | ModelCategory.Ordinal =>
        // Methods of EasyPredictModelWrapper for given prediction categories take offset as parameter.
        // Propagation of offset to EasyPredictModelWrapper was introduced with H2OSupervisedMOJOModel.
        // `lit(0.0)` represents a column with zero values (offset disabled) to ensure backward-compatibility of
        // MOJO models.
        flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*), lit(0.0)))
      case _ =>
        flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
    }
  }

  private[sparkling] def setParameters(mojoModel: MojoModel, modelJson: JsonObject, settings: H2OMOJOSettings): Unit = {
    val outputJson = modelJson.get("output").getAsJsonObject
    val modelCategory = extractModelCategory(outputJson)
    set(this.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    set(this.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    set(this.modelDetails -> getModelDetails(modelJson))
    set(this.trainingMetrics -> extractMetrics(outputJson, "training_metrics"))
    set(
      this.trainingMetricsObject ->
        extractMetricsObject(outputJson, "training_metrics", mojoModel._algoName, modelCategory))
    set(this.validationMetrics -> extractMetrics(outputJson, "validation_metrics"))
    set(
      this.validationMetricsObject ->
        extractMetricsObject(outputJson, "validation_metrics", mojoModel._algoName, modelCategory))
    set(this.crossValidationMetrics -> extractMetrics(outputJson, "cross_validation_metrics"))
    set(
      this.crossValidationMetricsObject ->
        extractMetricsObject(outputJson, "cross_validation_metrics", mojoModel._algoName, modelCategory))
    set(this.trainingParams -> extractParams(modelJson))
    set(this.modelCategory -> modelCategory.toString)
    set(this.scoringHistory -> extractScoringHistory(outputJson))
    set(this.featureImportances -> extractFeatureImportances(outputJson))
    set(this.featureTypes -> extractFeatureTypes(outputJson))
  }

  private[sparkling] def setEasyPredictModelWrapperConfiguration(
      config: EasyPredictModelWrapper.Config): EasyPredictModelWrapper.Config = {
    config.setConvertUnknownCategoricalLevelsToNa(this.getConvertUnknownCategoricalLevelsToNa())
    config.setConvertInvalidNumbersToNa(this.getConvertInvalidNumbersToNa())
    // always let H2O produce full output, filter later if required
    config.setUseExtendedOutput(true)
    config
  }

  private[sparkling] def loadEasyPredictModelWrapper(): EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    val mojo = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    config.setModel(mojo)
    setEasyPredictModelWrapperConfiguration(config)
    new EasyPredictModelWrapper(config)
  }

  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)
}

trait H2OMOJOModelUtils extends Logging {

  private def removeMetaField(json: JsonElement): JsonElement = {
    if (json.isJsonObject) {
      json.getAsJsonObject.remove("__meta")
      json.getAsJsonObject.entrySet().asScala.foreach(entry => removeMetaField(entry.getValue))
    }
    if (json.isJsonArray) {
      json.getAsJsonArray.asScala.foreach(removeMetaField)
    }
    json
  }

  protected def getModelJson(mojo: File): JsonObject = {
    val reader = MojoReaderBackendFactory.createReaderBackend(mojo.getAbsolutePath)
    ModelJsonReader.parseModelJson(reader)
  }

  protected def getModelDetails(modelJson: JsonObject): String = {
    val json = modelJson.get("output").getAsJsonObject

    if (json == null) {
      "Model details not available!"
    } else {
      removeMetaField(json)
      json.remove("domains")
      json.remove("help")
      val gson = new GsonBuilder().setPrettyPrinting().create
      val prettyJson = gson.toJson(json)
      prettyJson
    }
  }

  protected def extractMetrics(json: JsonObject, metricType: String): Map[String, Double] = {
    if (json.get(metricType).isJsonNull) {
      Map.empty
    } else {
      val metricGroup = json.getAsJsonObject(metricType)
      val fields = metricGroup.entrySet().asScala.map(_.getKey)
      val metrics = H2OMetric.values().flatMap { metric =>
        val metricName = metric.toString
        val fieldName = fields.find(field => field.replaceAll("_", "").equalsIgnoreCase(metricName))
        if (fieldName.isDefined) {
          Some(metric -> metricGroup.get(fieldName.get).getAsDouble)
        } else {
          None
        }
      }
      metrics.sorted(H2OMetricOrdering).map(pair => (pair._1.name(), pair._2)).toMap
    }
  }

  protected def extractMetricsObject(
      json: JsonObject,
      metricType: String,
      algoName: String,
      modelCategory: H2OModelCategory.Value): H2OMetrics = {
    val groupJson = json.get(metricType)
    if (groupJson.isJsonNull) {
      null
    } else {
      val metricGroup = groupJson.getAsJsonObject()
      H2OMetrics.loadMetrics(metricGroup, metricType, algoName, modelCategory)
    }
  }

  protected def extractParams(modelJson: JsonObject): Map[String, String] = {
    val parameters = modelJson.get("parameters").getAsJsonArray.asScala.toArray
    parameters
      .flatMap { param =>
        val name = param.getAsJsonObject.get("name").getAsString
        val value = param.getAsJsonObject.get("actual_value")
        val stringValue = stringifyJSON(value)
        stringValue.map(name -> _)
      }
      .sorted
      .toMap
  }

  protected def extractModelCategory(outputJson: JsonObject): H2OModelCategory.Value = {
    H2OModelCategory.fromString(outputJson.get("model_category").getAsString)
  }

  protected def extractFeatureTypes(outputJson: JsonObject): Map[String, String] = {
    val names = outputJson.getAsJsonArray("names").asScala.map(_.getAsString)
    val columnTypesJsonArray = outputJson.getAsJsonArray("column_types")
    if (columnTypesJsonArray != null) {
      val types = columnTypesJsonArray.asScala.map(_.getAsString)
      names.zip(types).toMap
    } else {
      Map.empty[String, String]
    }
  }

  protected def jsonFieldToDataFrame(outputJson: JsonObject, fieldName: String): DataFrame = {
    if (outputJson == null || !outputJson.has(fieldName) || outputJson.get(fieldName).isJsonNull) {
      null
    } else {
      try {
        val table = ModelJsonReader.readTable(outputJson, fieldName)
        val columnTypes = table.getColTypes.map {
          case ColumnType.LONG => LongType
          case ColumnType.INT => IntegerType
          case ColumnType.DOUBLE => DoubleType
          case ColumnType.FLOAT => FloatType
          case ColumnType.STRING => StringType
        }
        val columns = table.getColHeaders.zip(columnTypes).map {
          case (columnName, columnType) => StructField(columnName, columnType, nullable = true)
        }
        val schema = StructType(columns)
        val rows = (0 until table.rows()).map { rowId =>
          val rowData = (0 until table.columns())
            .map { colId =>
              table.getCell(colId, rowId) match {
                case str: String if table.getColTypes()(colId) == ColumnType.INT => Integer.parseInt(str)
                case str: String if table.getColTypes()(colId) == ColumnType.FLOAT => java.lang.Float.parseFloat(str)
                case value => value
              }
            }
            .toArray[Any]
          val row: Row = new GenericRowWithSchema(rowData, schema)
          row
        }.asJava
        SparkSessionUtils.active.createDataFrame(rows, schema)
      } catch {
        case e: Throwable =>
          logError(s"Unsuccessful try to extract '$fieldName' as a data frame from JSON representation.", e)
          null
      }
    }
  }

  protected def extractScoringHistory(outputJson: JsonObject): DataFrame = {
    val df = jsonFieldToDataFrame(outputJson, "scoring_history")
    if (df != null && df.columns.contains("")) df.drop("") else df
  }

  protected def extractFeatureImportances(outputJson: JsonObject): DataFrame = {
    jsonFieldToDataFrame(outputJson, "variable_importances")
  }

  private def stringifyJSON(value: JsonElement): Option[String] = {
    value match {
      case v: JsonPrimitive => Some(v.getAsString)
      case v: JsonArray =>
        val stringElements = v.asScala.flatMap(stringifyJSON)
        val arrayAsString = stringElements.mkString("[", ", ", "]")
        Some(arrayAsString)
      case _: JsonNull => None
      case v: JsonObject =>
        if (v.has("name")) {
          stringifyJSON(v.get("name"))
        } else {
          None
        }
    }
  }

  private object H2OMetricOrdering extends Ordering[(H2OMetric, Double)] {
    def compare(a: (H2OMetric, Double), b: (H2OMetric, Double)): Int = a._1.name().compare(b._1.name())
  }

}

object H2OMOJOModel
  extends H2OMOJOReadable[H2OMOJOModel]
  with H2OMOJOLoader[H2OMOJOModel]
  with H2OMOJOModelUtils
  with H2OMOJOModelFactory {

  override def createFromMojo(mojo: InputStream, uid: String, settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoFile = SparkSessionUtils.inputStreamToTempFile(mojo, uid, ".mojo")
    createFromMojo(mojoFile, uid, settings)
  }

  def createFromMojo(mojo: File, uid: String, settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoModel = Utils.getMojoModel(mojo)
    val model = createSpecificMOJOModel(uid, mojoModel._algoName, mojoModel._category)
    model.setSpecificParams(mojoModel)
    model.setMojo(mojo)
    val modelJson = getModelJson(mojo)
    model.setParameters(mojoModel, modelJson, settings)
    model
  }
}

abstract class H2OSpecificMOJOLoader[T <: ai.h2o.sparkling.ml.models.HasMojo: ClassTag]
  extends H2OMOJOReadable[T]
  with H2OMOJOLoader[T] {

  override def createFromMojo(mojo: InputStream, uid: String, settings: H2OMOJOSettings): T = {
    val mojoModel = H2OMOJOModel.createFromMojo(mojo, uid, settings)
    mojoModel match {
      case specificModel: T => specificModel
      case unexpectedModel =>
        throw new RuntimeException(
          s"The MOJO model can't be loaded " +
            s"as ${this.getClass.getSimpleName}. Use ${unexpectedModel.getClass.getSimpleName} instead!")
    }
  }
}

object H2OMOJOCache extends H2OMOJOBaseCache[MojoModel, H2OMOJOModel] {
  override def loadMojoBackend(mojo: File, model: H2OMOJOModel): MojoModel = Utils.getMojoModel(mojo)
}
