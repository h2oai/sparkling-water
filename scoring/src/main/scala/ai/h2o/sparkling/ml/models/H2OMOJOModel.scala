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

import _root_.hex.genmodel.attributes.ModelJsonReader
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import _root_.hex.genmodel.{GenModel, MojoReaderBackendFactory, PredictContributionsFactory}
import ai.h2o.sparkling.ml.params.NullableStringParam
import ai.h2o.sparkling.ml.utils.Utils
import com.google.gson.{GsonBuilder, JsonElement}
import hex.ModelCategory
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql._

import scala.collection.JavaConverters._

class H2OMOJOModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOModel] with H2OMOJOPrediction {

  protected final val modelDetails: NullableStringParam = new NullableStringParam(this, "modelDetails", "Raw details of this model.")

  setDefault(
    modelDetails -> null
  )

  def getModelDetails(): String = $(modelDetails)

  override protected def outputColumnName: String = getDetailedPredictionCol()

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient protected lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(Utils.getMojoModel(getMojoData()))
    config.setConvertUnknownCategoricalLevelsToNa(getConvertUnknownCategoricalLevelsToNa())
    config.setConvertInvalidNumbersToNa(getConvertInvalidNumbersToNa())
    if (canGenerateContributions(config.getModel)) {
      config.setEnableContributions(getWithDetailedPredictionCol())
    }
    // always let H2O produce full output, filter later if required
    config.setUseExtendedOutput(true)
    new EasyPredictModelWrapper(config)
  }

  private def canGenerateContributions(model: GenModel): Boolean = {
    model match {
      case _: PredictContributionsFactory =>
        val modelCategory = model.getModelCategory
        modelCategory == ModelCategory.Regression || modelCategory == ModelCategory.Binomial
      case _ => false
    }
  }

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


  protected def getModelDetails(mojoData: Array[Byte]): String = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)


    val modelOutputJson = ModelJsonReader.parseModelJson(reader).getAsJsonObject("output")
    if (modelOutputJson == null) {
      "Model details not available!"
    } else {
      removeMetaField(modelOutputJson)
      modelOutputJson.remove("domains")
      modelOutputJson.remove("help")
      val gson = new GsonBuilder().setPrettyPrinting().create
      val prettyJson = gson.toJson(modelOutputJson)
      prettyJson
    }
  }

}

object H2OMOJOModel extends H2OMOJOReadable[H2OMOJOModel] with H2OMOJOLoader[H2OMOJOModel] with H2OMOJOModelUtils {

  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoModel = Utils.getMojoModel(mojoData)

    val model = new H2OMOJOModel(uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    model.set(model.featuresCols -> mojoModel.features())
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.modelDetails -> getModelDetails(mojoData))
    model.set(model.predictionCol -> settings.predictionCol)
    model.set(model.detailedPredictionCol -> settings.detailedPredictionCol)
    model.set(model.withDetailedPredictionCol -> settings.withDetailedPredictionCol)
    model.setMojoData(mojoData)
    model
  }
}

