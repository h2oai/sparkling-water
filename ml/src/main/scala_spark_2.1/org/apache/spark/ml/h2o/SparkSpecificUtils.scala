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

package org.apache.spark.ml.h2o

import org.apache.spark.SparkContext
import org.apache.spark.ml.h2o.models.CrossSparkUtils
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.DefaultParamsReader
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject}

object SparkSpecificUtils extends CrossSparkUtils {

  private def getAndSetParams(
                               instance: Params,
                               metadata: Metadata,
                               skipParams: Option[List[String]] = None): Unit = {
    implicit val format = DefaultFormats
    metadata.params match {
      case JObject(pairs) =>
        pairs.foreach { case (paramName, jsonValue) =>
          if (skipParams == None || !skipParams.get.contains(paramName)) {
            val param = instance.getParam(paramName)
            val value = param.jsonDecode(compact(render(jsonValue)))
            instance.set(param, value)
          }
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Cannot recognize JSON metadata: ${metadata.metadataJson}.")
    }
  }

  override def setTransformerParams(metadata: DefaultParamsReader.Metadata, instance: Params, filteredParams: List[String]): Unit = {
    getAndSetParams(instance, metadata, Some(filteredParams))
  }

  override def getNumTaskForStage(sc: SparkContext, stageId: Int): Int = {
    sc.jobProgressListener.stageIdToInfo(stageId).numTasks
  }
}
