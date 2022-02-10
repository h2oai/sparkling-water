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
import hex.ensemble.StackedEnsembleModel.StackedEnsembleParameters
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset
import scala.reflect.ClassTag

/**
  * H2O Stacked Ensemble algorithm exposed via Spark ML pipelines.
  *
  */
class H2OStackedEnsemble(override val uid: String)
  extends H2OSupervisedAlgorithm[StackedEnsembleParameters]
  //with H2OTrainFramePreparation
  with H2OStackedEnsembleParams {

  def this() = this(Identifiable.randomUID(classOf[H2OStackedEnsemble].getSimpleName))

  private var baseModels: Array[H2OMOJOModel] = Array.empty

  def setBaseModels(models: Seq[H2OMOJOModel]): this.type  = {
    baseModels = models.toArray
    this
  }

  override def fit(dataset: Dataset[_]): H2OStackedEnsembleMOJOModel = {

    val baseModelIds:String = baseModels.map(m => m.mojoFileName).mkString("[", ",", "]")

    val (train, valid) = prepareDatasetForFitting(dataset)

    prepareH2OTrainFrameForFitting(train)

    val params = getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId, "model_id" -> convertModelIdToKey(getModelId())) ++
      valid.map { fr => Map("validation_frame" -> fr.frameId) }.getOrElse(Map()) ++
      Map("base_models" -> baseModelIds)

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
    result
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType =  schema

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def paramTag: ClassTag[StackedEnsembleParameters] = scala.reflect.classTag[StackedEnsembleParameters]

  override private[sparkling] def getInputCols(): Array[String] = getFeaturesCols()

  override private[sparkling] def setInputCols(value: Array[String]): this.type = setFeaturesCols(value)
}


