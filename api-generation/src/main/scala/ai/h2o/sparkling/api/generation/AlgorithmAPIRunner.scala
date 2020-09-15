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

import ai.h2o.sparkling.api.generation.common.{APIRunnerBase, AlgorithmConfigurations, AutoMLConfiguration, GridSearchConfiguration}

object AlgorithmAPIRunner
  extends APIRunnerBase
  with AlgorithmConfigurations
  with GridSearchConfiguration
  with AutoMLConfiguration {

  private val algorithmTemplates = Map("scala" -> scala.AlgorithmTemplate, "py" -> python.AlgorithmTemplate)

  private val problemSpecificAlgorithmTemplates =
    Map("scala" -> scala.ProblemSpecificAlgorithmTemplate, "py" -> python.ProblemSpecificAlgorithmTemplate)

  private val parameterTemplates = Map("scala" -> scala.ParametersTemplate, "py" -> python.ParametersTemplate)

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    for (substitutionContext <- parametersConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    for ((algorithmContext, parameterContext) <- algorithmConfiguration.zip(parametersConfiguration)) {
      val content = algorithmTemplates(languageExtension)(algorithmContext, Seq(parameterContext))
      writeResultToFile(content, algorithmContext, languageExtension, destinationDir)
    }

    val parametersConfigurationSequences = parametersConfiguration.map(Seq(_))
    val specificAlgorithmCombinations = problemSpecificAlgorithmConfiguration.zip(parametersConfigurationSequences) :+
      (problemSpecificAutoMLAlgorithmContext, autoMLParameterConfiguration)

    for ((algorithmContext, parameterContexts) <- specificAlgorithmCombinations) {
      val classificationAlgorithmContext = algorithmContext.copy(
        entityName = algorithmContext.parentEntityName + "Classifier",
        namespace = algorithmContext.parentNamespace + ".classification")
      val content = problemSpecificAlgorithmTemplates(languageExtension)(
        "classification",
        classificationAlgorithmContext,
        parameterContexts)
      writeResultToFile(content, classificationAlgorithmContext, languageExtension, destinationDir)
    }

    for ((algorithmContext, parameterContexts) <- specificAlgorithmCombinations) {
      val regressionAlgorithmContext = algorithmContext.copy(
        entityName = algorithmContext.parentEntityName + "Regressor",
        namespace = algorithmContext.parentNamespace + ".regression")
      val content = problemSpecificAlgorithmTemplates(languageExtension)(
        "regression",
        regressionAlgorithmContext,
        parameterContexts)
      writeResultToFile(content, regressionAlgorithmContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- autoMLParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    if (languageExtension != "scala") {
      val content = algorithmTemplates(languageExtension)(autoMLAlgorithmContext, autoMLParameterConfiguration)
      writeResultToFile(content, autoMLAlgorithmContext, languageExtension, destinationDir)
    }

    for (substitutionContext <- gridSearchParameterConfiguration) {
      val content = parameterTemplates(languageExtension)(substitutionContext)
      writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
    }

    if (languageExtension != "scala") {
      val content = algorithmTemplates(languageExtension)(gridSearchAlgorithmContext, gridSearchParameterConfiguration)
      writeResultToFile(content, gridSearchAlgorithmContext, languageExtension, destinationDir)
    }
  }
}
