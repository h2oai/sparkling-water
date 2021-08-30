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

import ai.h2o.sparkling.ml.metrics.MetricsDescription
import org.apache.spark.ml.param.Params

object MetricsTemplate {
  def apply(metricsClass: Class[_]): String = {
    val caption = s"${metricsClass.getSimpleName} Class"
    val dashes = caption.toCharArray.map(_ => '-').mkString
    val content = getMetricsContent(metricsClass)
    val description = metricsClass.getAnnotation[MetricsDescription](classOf[MetricsDescription]).description()

    s""".. _metrics_${metricsClass.getSimpleName}:
       |
       |$caption
       |$dashes
       |
       |$description
       |
       |**Getter Methods**
       |
       |$content
       |""".stripMargin
  }

  private def getMetricsContent(metricsClass: Class[_]): String = {
    val metricsInstance = metricsClass.newInstance().asInstanceOf[Params]
    metricsInstance.params
      .map { param =>
        val lowerCaseName = param.name.toLowerCase
        val method = metricsClass.getMethods.find(_.getName.toLowerCase == "get" + lowerCaseName).get

        s"""${method.getName}()
           |  **Returns:** ${param.doc.replace("\n ", "\n\n  - ")}
           |
           |  ${generateType(method.getReturnType())}
           |""".stripMargin
      }
      .mkString("\n")
  }

  private def generateType(returnType: Class[_]): String = returnType.getSimpleName match {
    case "DataFrame" | "Dataset" => "*Type:* ``DataFrame``"
    case "String" => "*Scala type:* ``String``, *Python type:* ``string``, *R type:* ``character``"
    case "Double" | "double" => "*Scala type:* ``Double``, *Python type:* ``float``, *R type:* ``numeric``"
    case "Float" | "float" => "*Scala type:* ``Float``, *Python type:* ``float``, *R type:* ``numeric``"
    case "Long" | "long" => "*Scala type:* ``Long``, *Python type:* ``int``, *R type:* ``integer``"
  }
}
