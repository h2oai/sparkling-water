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

package ai.h2o.sparkling.model

import java.io.File

import ai.h2o.sparkling.backend.external.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.job.H2OJob
import ai.h2o.sparkling.utils.ScalaUtils._
import com.google.gson.{Gson, JsonElement}
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OConf

import scala.collection.mutable

case class H2OModel(key: String, algoName: String)

private[sparkling] object H2OModel extends RestCommunication {


  private[sparkling] def trainAutoML(conf: H2OConf,
                                     H2OParamNameToValueMap: Map[String, Any],
                                     trainKey: String,
                                     validKey: Option[String]): H2OModel = {
    val params = mutable.Map("training_frame" -> trainKey) ++ H2OParamNameToValueMap
    validKey.foreach { key => params += ("validation_frame" -> key) }

    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val content = withResource(readURLContent(endpoint, "POST", s"/99/AutoMLBuilder", conf, params.toMap, asJSON = true)) { response =>
      IOUtils.toString(response)
    }

    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val modelId = job.get("dest").getAsJsonObject.get("name").getAsString
    H2OModel(modelId, "")
  }

  private[sparkling] def trainSimpleModel(conf: H2OConf,
                                          algoName: String,
                                          H2OParamNameToValueMap: Map[String, Any],
                                          trainKey: String,
                                          validKey: Option[String]): H2OModel = {
    val params = mutable.Map("training_frame" -> trainKey) ++ H2OParamNameToValueMap
    validKey.foreach { key => params += ("validation_frame" -> key) }

    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val content = withResource(readURLContent(endpoint, "POST", s"/3/ModelBuilders/$algoName", conf, params.toMap)) { response =>
      IOUtils.toString(response)
    }

    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val modelId = job.get("dest").getAsJsonObject.get("name").getAsString
    H2OModel(modelId, algoName)
  }

  private[sparkling] def downloadMOJOData(conf: H2OConf, model: H2OModel): Array[Byte] = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, model.key)
    downloadBinaryURLContent(endpoint, s"/3/Models/${model.key}/mojo", conf, target)
    import java.nio.file.Files
    Files.readAllBytes(target.toPath)
  }
}
