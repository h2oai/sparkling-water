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

package ai.h2o.sparkling.doc.generation

import org.apache.spark.ml.param.Params

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object ModelDetailsTemplate {
  def apply(algorithm: Class[_], mojoModel: Class[_]): String = {
    val caption = s"Details of ${mojoModel.getSimpleName}"
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val content = getModelDetailsContent(algorithm, mojoModel)
    s""".. _model_details_${mojoModel.getSimpleName}:
       |
       |$caption
       |$dashes
       |
       |$content
     """.stripMargin
  }

  private def getModelDetailsContent(algorithm: Class[_], mojoModel: Class[_]): String = {
    val algorithmInstance = algorithm.newInstance().asInstanceOf[Params]
    val mojoModelInstance = mojoModel.getConstructor(classOf[String]).newInstance("uid").asInstanceOf[Params]
    val mojoParamNames = mojoModelInstance.params.map(_.name)
    val algorithmParamNames = algorithmInstance.params.map(_.name)
    val relevantParamNames = mojoParamNames.diff(algorithmParamNames)
    if (relevantParamNames.nonEmpty) {
      relevantParamNames
        .map { paramName =>
          val param = mojoModelInstance.getParam(paramName)

          s"""get${param.name.capitalize}()
             |  ${param.doc.replace(" \n", "").replace("\n ", "\n\n  - ")}

             |""".stripMargin
        }
        .mkString("\n")
    } else {
      "No model detail methods."
    }
  }
}
