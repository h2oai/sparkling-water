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

object ParametersTocTreeTemplate {
  def apply(algorithms: Seq[Class[_]], featureTransformers: Seq[Class[_]]): String = {
    val algorithmItems = algorithms.map(algorithm => s"   parameters_${algorithm.getSimpleName}").mkString("\n")
    val featureItems = featureTransformers.map(feature => s"   parameters_${feature.getSimpleName}").mkString("\n")
    s"""Algorithm Parameters
       |====================
       |
       |**Algorithms**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$algorithmItems
       |
       |**Feature Transformers**
       |
       |.. toctree::
       |   :maxdepth: 2
       |
       |$featureItems
    """.stripMargin
  }
}
