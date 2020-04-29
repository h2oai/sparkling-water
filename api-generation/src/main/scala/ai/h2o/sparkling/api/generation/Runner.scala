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

package ai.h2o.sparkling.api.generation

import java.io.{File, PrintWriter}

import ai.h2o.sparkling.api.generation.common._
import ai.h2o.sparkling.api.generation.scala.ParametersTemplate
import ai.h2o.sparkling.utils.ScalaUtils._
import hex.schemas._
import hex.tree.gbm.GBMModel.GBMParameters
import hex.glm.GLMModel.GLMParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters

object Runner {
  private def parametersConfiguration: Seq[ParameterSubstitutionContext] = Seq(
    ("H2OXGBoostParams", classOf[XGBoostV3.XGBoostParametersV3], classOf[XGBoostParameters])
  ).map { case (entityName, h2oSchemaClass: Class[_], h2oParametersClass: Class[_]) =>
    ParameterSubstitutionContext(
      CommonSubstitutionContext(
        namespace = "ai.h2o.sparkling.ml.params",
        entityName = entityName,
        inheritedEntities = Seq(
          s"H2OAlgoSupervisedParams[${h2oParametersClass.getSimpleName}]",
          "H2OTreeBasedSupervisedMOJOParams"),
        imports = Seq("ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper.getValidatedEnumValue")),
      h2oSchemaClass,
      h2oParametersClass
    )
  }

  def main(args: Array[String]): Unit = {
    val language = args(0)
    val destinationDir = args(1)

    for (substitutionContext <- parametersConfiguration) {
      val fileName = substitutionContext.commonContext.entityName
      val namespacePath = substitutionContext.commonContext.namespace.replace('.', '/')
      new File(destinationDir, namespacePath).mkdirs()
      val destinationFile = new File(destinationDir,s"$fileName.$language")
      withResource(new PrintWriter(destinationFile)) { outputStream =>
        val content = ParametersTemplate(substitutionContext)
        outputStream.print(content)
      }
    }
  }
}
