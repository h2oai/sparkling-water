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

import ai.h2o.sparkling.backend.external.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.job.H2OJob
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OAlgoCommonParams
import ai.h2o.sparkling.model.H2OModel
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import com.google.gson.{Gson, JsonElement}
import hex.Model
import org.apache.commons.io.IOUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import water.exceptions.H2OModelBuilderIllegalArgumentException
import water.support.ModelSerializationSupport
import water.{DKV, H2O, Key}

import scala.reflect.{ClassTag, classTag}

/**
 * Base class for H2O algorithm wrapper as a Spark transformer.
 */
abstract class H2OAlgorithm[B <: H2OBaseModelBuilder : ClassTag, M <: H2OBaseModel, P <: Model.Parameters : ClassTag]
  extends Estimator[H2OMOJOModel]
    with H2OAlgoCommonUtils
    with DefaultParamsWritable
    with H2OAlgoCommonParams[P]
    with RestCommunication {

  protected def prepareH2OTrainFrameForFitting(trainFrameKey: String): Unit = {}

  private def fitOverRest(trainKey: String, validKey: Option[String]): Array[Byte] = {
    prepareH2OTrainFrameForFitting(trainKey)
    val conf = H2OContext.ensure().getConf
    val params = getH2OAlgoRESTParams() ++
      Map("training_frame" -> trainKey) ++
      validKey.map { key => Map("validation_frame" -> key) }.getOrElse(Map())

    val algoName = parameters.algoName().toLowerCase
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val content = withResource(readURLContent(endpoint, "POST", s"/3/ModelBuilders/$algoName", conf, params)) { response =>
      IOUtils.toString(response)
    }

    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val modelId = job.get("dest").getAsJsonObject.get("name").getAsString
    H2OModel(modelId).getOrDownloadMojoData()
  }

  private def fitOverClient(trainKey: String, validKey: Option[String]): Array[Byte] = {
    updateH2OParams()
    parameters._train = DKV.getGet[Frame](trainKey)._key
    parameters._valid = validKey.map(DKV.getGet[Frame](_)._key).orNull
    prepareH2OTrainFrameForFitting(trainKey)
    water.DKV.put(parameters._train.get())
    val binaryModel = trainModel(parameters)
    ModelSerializationSupport.getMojoData(binaryModel)
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val mojoData = if (RestApiUtils.isRestAPIBased()) {
      fitOverRest(trainKey, validKey)
    } else {
      fitOverClient(trainKey, validKey)
    }
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(parameters.algoName()),
      modelSettings,
      internalFeatureCols)
  }

  private def trainModel(params: P): H2OBaseModel = {
    val modelId = getModelId()
    val algoClass = classTag[B].runtimeClass
    val parameterClass = classTag[P].runtimeClass
    val builder = if (modelId == null || modelId.isEmpty) {
      val constructor = algoClass.getConstructor(parameterClass)
      constructor.newInstance(params)
    } else {
      val constructor = algoClass.getConstructor(parameterClass, classOf[Key[M]])
      constructor.newInstance(params, convertModelIdToKey(modelId))
    }
    try {
      builder.asInstanceOf[B].trainModel().get()
    } catch {
      case e: H2OModelBuilderIllegalArgumentException
        if e.getMessage.contains("There are no usable columns to generate") =>
        throw new IllegalArgumentException(s"H2O could not use any of the specified feature" +
          s" columns: '${getFeaturesCols().mkString(", ")}'. H2O ignores constant columns, are all the columns constants?", e)
    }
  }

  private def convertModelIdToKey(modelId: String): Key[M] = {
    val key = Key.make[M](modelId)
    if (H2O.containsKey(key)) {
      val replacement = findAlternativeKey(modelId)
      logWarning(s"Model id '$modelId' is already used by a different H2O model. Replacing the original id with '$replacement' ...")
      replacement
    } else {
      key
    }
  }

  private def findAlternativeKey(modelId: String): Key[M] = {
    var suffixNumber = 0
    var replacement: Key[M] = null
    do {
      suffixNumber = suffixNumber + 1
      replacement = Key.make[M](s"${modelId}_$suffixNumber")
    } while (H2O.containsKey(replacement))
    replacement
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
