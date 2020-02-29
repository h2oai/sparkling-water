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
import com.google.common.base.CaseFormat
import com.google.gson.{Gson, JsonElement}
import hex.Model
import hex.Model.Parameters
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OConf
import org.apache.spark.ml.param.ParamMap
import water.util.PojoUtils

import scala.collection.mutable

case class H2OModel(key: String, algoName: String)

object H2OModel extends RestCommunication {
  def trainModel[P <: Model.Parameters](conf: H2OConf, sparkPrams: ParamMap, sparkToH2OParamsExceptions: Map[String, String],
                                        h2oParams: P, trainKey: String, validKey: Option[String]): H2OModel = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)

    val algoName = h2oParams.algoName().toLowerCase
    val params = prepareFinalParams(sparkPrams, h2oParams, sparkToH2OParamsExceptions, trainKey, validKey)

    val content = withResource(readURLContent(endpoint, "POST", s"/3/ModelBuilders/$algoName", conf, params)) { response =>
      IOUtils.toString(response)
    }

    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val modelId = job.get("dest").getAsJsonObject.get("name").getAsString
    H2OModel(modelId, algoName)
  }


  def downloadMOJOData(conf: H2OConf, model: H2OModel): Array[Byte] = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val sparkTmpDir = Utils.createTempDir(Utils.getLocalDir(conf.sparkConf))
    val target = new File(sparkTmpDir, model.key)
    downloadBinaryURLContent(endpoint, s"/3/Models/${model.key}/mojo", conf, target)
    import java.nio.file.Files
    Files.readAllBytes(target.toPath)
  }

  private def prepareFinalParams[P <: Model.Parameters](sparkParams: ParamMap, h2oParams: P,
                                                        sparkToH2OParamsExceptions: Map[String, String],
                                                        trainKey: String,
                                                        validKey: Option[String]): Map[String, String] = {

    val convertedParams = sparkParams.toSeq.map { paramPair =>
      val name = sparkToH2OParamsExceptions.getOrElse(paramPair.param.name, paramPair.param.name)
      val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
      (underscoredName, paramPair.value)
    }.filter(pair => pair._2 != null && fieldExistsOnH2OParams(h2oParams, s"_${pair._1}"))
      .map { pair =>
        (pair._1, pair._2 match {
          case map: java.util.AbstractMap[_, _] => stringifyMap(map)
          case arr: Array[_] => stringifyArray(arr)
          case simple => simple.toString
        })
      }

    val parameters = mutable.Map(convertedParams: _*)
    parameters += "training_frame" -> trainKey
    validKey.foreach { key => parameters += ("validation_frame" -> key) }
    parameters.toMap
  }

  private def stringifyArray(arr: Array[_]): String = {
    arr.mkString("[", ",", "]")
  }

  private def stringifyMap(map: java.util.AbstractMap[_, _]): String = {
    import scala.collection.JavaConversions._
    stringifyArray(map.toSeq.map(pair => s"{'key': ${pair._1}, 'value':${pair._2}}").toArray)
  }


  private def fieldExistsOnH2OParams(obj: Parameters, fieldName: String): Boolean = {
    try {
      PojoUtils.getFieldEvenInherited(obj, fieldName)
      true
    } catch {
      case _: Throwable => false
    }
  }
}
