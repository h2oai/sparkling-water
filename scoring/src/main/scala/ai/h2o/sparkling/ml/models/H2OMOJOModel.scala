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

import java.io.ByteArrayInputStream

import _root_.hex.genmodel.algos.tree.SharedTreeMojoModel
import _root_.hex.genmodel.algos.xgboost.XGBoostMojoModel
import _root_.hex.genmodel.attributes.ModelJsonReader
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import _root_.hex.genmodel.{GenModel, MojoModel, MojoReaderBackendFactory, PredictContributionsFactory}
import ai.h2o.sparkling.ml.internals.{H2OMetric, H2OModelCategory}
import ai.h2o.sparkling.ml.params.{MapStringDoubleParam, MapStringStringParam, NullableStringParam}
import ai.h2o.sparkling.ml.utils.Utils
import com.google.gson._
import hex.ModelCategory
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class H2OMOJOModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOModel] with H2OMOJOPrediction {
  H2OMOJOCache.startCleanupThread()
  protected final val modelDetails: NullableStringParam =
    new NullableStringParam(this, "modelDetails", "Raw details of this model.")
  protected final val trainingMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "trainingMetrics", "Training metrics.")
  protected final val validationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "validationMetrics", "Validation metrics.")
  protected final val crossValidationMetrics: MapStringDoubleParam =
    new MapStringDoubleParam(this, "crossValidationMetrics", "Cross Validation metrics.")
  protected final val trainingParams: MapStringStringParam =
    new MapStringStringParam(this, "trainingParams", "Training params")
  protected final val modelCategory: NullableStringParam =
    new NullableStringParam(this, "modelCategory", "H2O's model category")
  setDefault(
    modelDetails -> null,
    trainingMetrics -> Map.empty[String, Double],
    validationMetrics -> Map.empty[String, Double],
    crossValidationMetrics -> Map.empty[String, Double],
    trainingParams -> Map.empty[String, String],
    modelCategory -> null)

  def getTrainingMetrics(): Map[String, Double] = $(trainingMetrics)

  def getValidationMetrics(): Map[String, Double] = $(validationMetrics)

  def getCrossValidationMetrics(): Map[String, Double] = $(crossValidationMetrics)

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
  def getTrainingParams(): Map[String, String] = $(trainingParams)

  def getModelCategory(): String = $(modelCategory)

  def getModelDetails(): String = $(modelDetails)

  def getDomainValues(): Map[String, Array[String]] = {
    val mojoBackend = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
    val columns = mojoBackend.m.getNames
    columns.map(col => col -> mojoBackend.m.getDomainValues(col)).toMap
  }

  def setSpecificParams(mojoModel: MojoModel): H2OMOJOModel = this

  override protected def outputColumnName: String = getDetailedPredictionCol()

  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val baseDf = applyPredictionUdf(dataset, _ => getPredictionUDF())

    val withPredictionDf = baseDf.withColumn(getPredictionCol(), extractPredictionColContent())

    if (getWithDetailedPredictionCol()) {
      withPredictionDf
    } else {
      withPredictionDf.drop(getDetailedPredictionCol())
    }
  }

  protected override def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = flatDataFrame.columns.intersect(inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojoData, this)
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
}

trait H2OMOJOModelUtils {

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

  protected def getModelJson(mojoData: Array[Byte]): JsonObject = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
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

  protected def extractAllMetrics(
      modelJson: JsonObject): (Map[String, Double], Map[String, Double], Map[String, Double]) = {
    val json = modelJson.get("output").getAsJsonObject
    val trainingMetrics = extractMetrics(json, "training_metrics")
    val validationMetrics = extractMetrics(json, "validation_metrics")
    val crossValidationMetrics = extractMetrics(json, "cross_validation_metrics")
    (trainingMetrics, validationMetrics, crossValidationMetrics)
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

  protected def extractModelCategory(modelJson: JsonObject): H2OModelCategory.Value = {
    val json = modelJson.get("output").getAsJsonObject
    H2OModelCategory.fromString(json.get("model_category").getAsString)
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

object H2OMOJOModel extends H2OMOJOReadable[H2OMOJOModel] with H2OMOJOLoader[H2OMOJOModel] with H2OMOJOModelUtils {

  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoModel = Utils.getMojoModel(mojoData)
    val model = mojoModel match {
      case _: SharedTreeMojoModel | _: XGBoostMojoModel => new H2OTreeBasedSupervisedMOJOModel(uid)
      case m if m.isSupervised => new H2OSupervisedMOJOModel(uid)
      case _ => new H2OUnsupervisedMOJOModel(uid)
    }

    model.setSpecificParams(mojoModel)

    val modelJson = getModelJson(mojoData)
    val (trainingMetrics, validationMetrics, crossValidationMetrics) = extractAllMetrics(modelJson)
    val modelDetails = getModelDetails(modelJson)
    val modelCategory = extractModelCategory(modelJson)
    val trainingParams = extractParams(modelJson)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    model.set(model.featuresCols -> mojoModel.features())
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.modelDetails -> modelDetails)
    model.set(model.predictionCol -> settings.predictionCol)
    model.set(model.trainingMetrics -> trainingMetrics)
    model.set(model.validationMetrics -> validationMetrics)
    model.set(model.crossValidationMetrics -> crossValidationMetrics)
    model.set(model.trainingParams -> trainingParams)
    model.set(model.modelCategory -> modelCategory.toString)
    model.set(model.detailedPredictionCol -> settings.detailedPredictionCol)
    model.set(model.withDetailedPredictionCol -> settings.withDetailedPredictionCol)
    model.setMojoData(mojoData)
    model
  }

  // Internal method used only within Sparkling Water pipelines.
  // When H2OMOJOModel is created from existing mojo created in H2O-3, we set features names as features stored in mojo
  // (they are not nested and structured), but as in Spark, data frames can be nested, we need to handle it
  private[h2o] def createFromMojo(
      mojoData: Array[Byte],
      uid: String,
      settings: H2OMOJOSettings,
      originalFeatures: Array[String]): H2OMOJOModel = {
    val model = createFromMojo(mojoData, uid, settings)
    // Override the feature cols with the original features as Spark sees them.
    // Internally, we expand the arrays and vectors
    model.set(model.featuresCols -> originalFeatures)
  }
}

abstract class H2OSpecificMOJOLoader[T <: ai.h2o.sparkling.ml.models.HasMojoData: ClassTag]
  extends H2OMOJOReadable[T]
  with H2OMOJOLoader[T] {

  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): T = {
    val mojoModel = H2OMOJOModel.createFromMojo(mojoData, uid, settings)
    mojoModel match {
      case specificModel: T => specificModel
      case unexpectedModel =>
        throw new RuntimeException(
          s"The MOJO model can't be loaded " +
            s"as ${this.getClass.getSimpleName}. Use ${unexpectedModel.getClass.getSimpleName} instead!")
    }
  }
}

object H2OMOJOCache extends H2OMOJOBaseCache[EasyPredictModelWrapper, H2OMOJOModel] {

  private def canGenerateContributions(model: GenModel): Boolean = {
    model match {
      case _: PredictContributionsFactory =>
        val modelCategory = model.getModelCategory
        modelCategory == ModelCategory.Regression || modelCategory == ModelCategory.Binomial
      case _ => false
    }
  }

  override def loadMojoBackend(mojoData: Array[Byte], model: H2OMOJOModel): EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(Utils.getMojoModel(mojoData))
    config.setConvertUnknownCategoricalLevelsToNa(model.getConvertUnknownCategoricalLevelsToNa())
    config.setConvertInvalidNumbersToNa(model.getConvertInvalidNumbersToNa())
    if (canGenerateContributions(config.getModel)) {
      config.setEnableContributions(model.getWithDetailedPredictionCol())
    }
    // always let H2O produce full output, filter later if required
    config.setUseExtendedOutput(true)
    new EasyPredictModelWrapper(config)
  }
}
