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

package ai.h2o.sparkling.ml.internals

import java.io.{File, FileInputStream}

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.utils.ScalaUtils._
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OContext
import water.api.schemas3.ModelsV3

private[sparkling] class H2OModel private (val modelId: String) extends RestCommunication {
  private val conf = H2OContext.ensure("H2OContext needs to be running!").getConf

  private[sparkling] def downloadMojo(): File = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, this.modelId)
    downloadBinaryURLContent(endpoint, s"/3/Models/${this.modelId}/mojo", conf, target)
    target
  }

  private[sparkling] def toMOJOModel(
      uid: String,
      settings: H2OMOJOSettings,
      originalFeatures: Array[String]): H2OMOJOModel = {
    val mojo = downloadMojo()
    try {
      withResource(new FileInputStream(mojo)) { inputStream =>
        H2OMOJOModel.createFromMojo(inputStream, uid, settings, originalFeatures)
      }
    } finally {
      mojo.delete()
    }
  }
}

private[sparkling] object H2OModel extends RestCommunication {

  private[sparkling] def listAllModels(): Array[String] = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val models = query[ModelsV3](endpoint, "/3/Models", conf)
    models.models.map(_.model_id.name)
  }

  private[sparkling] def modelExists(modelId: String): Boolean = listAllModels().contains(modelId)

  def apply(modelId: String): H2OModel = new H2OModel(modelId)
}
