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

package ai.h2o.sparkling.api.generation.r

import java.lang.reflect.Method

object ConfigurationTemplate extends ((Array[Method], Array[Method], Class[_]) => String) {

  def apply(getters: Array[Method], setters: Array[Method], entity: Class[_]): String = {

    s"""#
       |#
       |# Licensed to the Apache Software Foundation (ASF) under one or more
       |# contributor license agreements.  See the NOTICE file distributed with
       |# this work for additional information regarding copyright ownership.
       |# The ASF licenses this file to You under the Apache License, Version 2.0
       |# (the "License"); you may not use this file except in compliance with
       |# the License.  You may obtain a copy of the License at
       |#
       |#    http://www.apache.org/licenses/LICENSE-2.0
       |#
       |# Unless required by applicable law or agreed to in writing, software
       |# distributed under the License is distributed on an "AS IS" BASIS,
       |# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       |# See the License for the specific language governing permissions and
       |# limitations under the License.
       |#
       |
       |if (!exists("ConfUtils.getOption", mode = "function")) source(file.path("R", "ConfUtils.R"))
       |
       |#' @export ${entity.getSimpleName}
       |${entity.getSimpleName} <- setRefClass("${entity.getSimpleName}", methods = list(
       |
       |    #
       |    # Getters
       |    #
       |
       |${generateGetters(getters)},
       |
       |    #
       |    # Setters
       |    #
       |
       |${generateSetters(setters)}
       |))""".stripMargin
  }

  private def generateGetters(getters: Array[Method]): String = {
    getters.map(generateGetter).mkString(",\n\n")
  }

  private def generateSetters(setters: Array[Method]): String = {
    setters.map(generateSetter).mkString(",\n\n")
  }

  private def generateGetter(m: Method): String = {
    m.getReturnType match {
      case clz if clz.getName == "scala.Option" =>
        s"""    ${m.getName} = function() { ConfUtils.getOption(invoke(jconf, "${m.getName}")) }"""
      case _ => s"""    ${m.getName} = function() { invoke(jconf, "${m.getName}") }"""
    }
  }

  private def generateSetter(m: Method): String = {
    if (m.getParameterCount == 0) {
      s"""    ${m.getName} = function() { invoke(jconf, "${m.getName}"); .self }"""
    } else {
      s"""    ${m.getName} = function(value) { invoke(jconf, "${m.getName}", value); .self }"""
    }
  }
}
