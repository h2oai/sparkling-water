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

package ai.h2o.sparkling.api.generation.scala

import ai.h2o.sparkling.api.generation.common.{AlgorithmSubstitutionContext, EntitySubstitutionContext}

object MOJOModelFactoryTemplate extends ((Seq[AlgorithmSubstitutionContext]) => String) with ScalaEntityTemplate {

  def apply(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {

    val entitySubstitutionContext = EntitySubstitutionContext(
      mojoSubstitutionContexts.head.namespace,
      "H2OMOJOModelFactory",
      inheritedEntities = Seq.empty,
      imports = Seq.empty)

    generateEntity(entitySubstitutionContext, "trait") {
      s"""  protected def createSpecificMOJOModel(uid: String, algorithmName: String, category: hex.ModelCategory): H2OMOJOModel = {
         |    (algorithmName, category) match {
         |      case (_, hex.ModelCategory.AutoEncoder) => new H2OAutoEncoderMOJOModel(uid)
         |${generatePatternMatchingCases(mojoSubstitutionContexts)}
         |      case _ => new H2OAlgorithmMOJOModel(uid)
         |    }
         |  }""".stripMargin
    }
  }

  private def generatePatternMatchingCases(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {
    mojoSubstitutionContexts
      .map { mojoSubstitutionContext =>
        val algorithmName = mojoSubstitutionContext.entityName
          .replace("H2O", "")
          .replace("MOJOModel", "")
          .toLowerCase
        s"""      case ("$algorithmName", _) => new ${mojoSubstitutionContext.entityName}(uid)"""
      }
      .mkString("\n")
  }
}
