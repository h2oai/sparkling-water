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
package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel, H2OStackedEnsembleMOJOModel}
import ai.h2o.sparkling.ml.params.H2OStackedEnsembleParams
import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.ensemble.StackedEnsembleModel.StackedEnsembleParameters
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset

import scala.collection.JavaConverters
import scala.reflect.ClassTag

/**
  * H2O Stacked Ensemble algorithm exposed via Spark ML pipelines.
  *
  */
class H2OStackedEnsemble(override val uid: String)
  extends H2OSupervisedAlgorithm[StackedEnsembleParameters]
  with H2OStackedEnsembleParams {

  def this() = this(Identifiable.randomUID(classOf[H2OStackedEnsemble].getSimpleName))

  private var baseModelsIds: Seq[String] = Seq.empty

  private var deleteBaseModels = true

  def setBaseModels(models: Seq[H2OMOJOModel]): this.type  = {
    checkBaseModelParameters(models)
    setBaseModelsIds(models.map(m => m.mojoFileName))
  }

  def setBaseModelsIds(modelIds: Seq[String]): this.type  = {

    if(!modelIds.forall(id => H2OBinaryModel.exists(id))) {
      throw new IllegalArgumentException(
        "Base models need to be fit first with the 'keepBinaryModels' parameter " +
          "set to true in order to access binary model.")
    }

    baseModelsIds = modelIds
    this
  }

  def setBaseModelsIds(modelIds: java.util.List[String]): this.type = {
    setBaseModelsIds(JavaConverters.asScalaBuffer(modelIds))
  }

  def setDeleteBaseModels(deleteModels: Boolean): this.type  = {
    deleteBaseModels = deleteModels
    this
  }

  override def fit(dataset: Dataset[_]): H2OStackedEnsembleMOJOModel = {

    if (baseModelsIds.length < 2) {
      throw new IllegalArgumentException("Algorithm needs at least two base models.")
    }

    val (train, valid) = prepareDatasetForFitting(dataset)

    prepareH2OTrainFrameForFitting(train)

    val params = getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId,
        "model_id" -> convertModelIdToKey(getModelId()),
        "base_models" -> baseModelsIds.mkString("[", ",", "]")) ++
      valid.map { fr => Map("validation_frame" -> fr.frameId) }.getOrElse(Map())

    val modelId = trainAndGetDestinationKey(s"/99/ModelBuilders/stackedensemble", params)
    val model = H2OModel(modelId)
    val withCrossValidationModels = if (hasParam("keepCrossValidationModels")) {
      getOrDefault(getParam("keepCrossValidationModels")).asInstanceOf[Boolean]
    } else {
      false
    }

    val result = model
      .toMOJOModel(createMOJOUID(), createMOJOSettings(), withCrossValidationModels)
      .asInstanceOf[H2OStackedEnsembleMOJOModel]

    if (H2OContext.get().forall(_.getConf.isModelPrintAfterTrainingEnabled)) {
      println(result)
    }

    deleteRegisteredH2OFrames()
    if (getKeepBinaryModels()) {
      val downloadedModel = downloadBinaryModel(modelId, H2OContext.ensure().getConf)
      binaryModel = Some(H2OBinaryModel.read("file://" + downloadedModel.getAbsolutePath, Some(modelId)))
    } else {
      model.tryDelete()
    }

    if (deleteBaseModels) {
      baseModelsIds.foreach(H2OModel(_).tryDelete())
    }

    result
  }

  def checkBaseModelParameters(models: Seq[H2OMOJOModel]) = {

    if (!haveModelsSameParamValue("nfolds", models)) {
      throw new IllegalArgumentException(
        "Base models need to have consistent number of folds.")
    }

    if (!haveModelsSameParamValue("foldAssignment", models)) {
      throw new IllegalArgumentException(
        "Base models need to have consistent fold assignment scheme.")
    }

    if (!haveModelsParamValue("keepCrossValidationPredictions", models, true)) {
      throw new IllegalArgumentException(
        "Base models need to be fit first with the 'keepCrossValidationPredictions' parameter " +
          "set to true in order to allow access to cross validations.")
    }
  }

  private def haveModelsSameParamValue(paramName:String, models: Seq[H2OMOJOModel]):Boolean = {
    val firstModel = models.head
    val param = firstModel.getParam(paramName)
    val firstValue = firstModel.getOrDefault(param)
    haveModelsParamValue(paramName, models, firstValue)
  }

  private def haveModelsParamValue(paramName: String, models: Seq[H2OMOJOModel], value: Any) = {
    models.forall(m => m.getOrDefault(m.getParam(paramName)) == value)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =  schema

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def paramTag: ClassTag[StackedEnsembleParameters] = scala.reflect.classTag[StackedEnsembleParameters]

  override private[sparkling] def getInputCols(): Array[String] = getFeaturesCols()

  override private[sparkling] def setInputCols(value: Array[String]): this.type = setFeaturesCols(value)
}

object H2OStackedEnsemble extends H2OParamsReadable[H2OStackedEnsemble]
