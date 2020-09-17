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

import ai.h2o.sparkling.api.generation.common._

object MOJOModelAPIRunner extends APIRunnerBase with AutoMLConfiguration {

  private val mojoTemplates = Map("scala" -> scala.MOJOModelTemplate, "py" -> python.MOJOModelTemplate)
  private val mojoFactoryTemplates =
    Map("scala" -> scala.MOJOModelFactoryTemplate, "py" -> python.MOJOModelFactoryTemplate)

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    val mojoConfiguration = algorithmConfiguration.map { algorithmContext =>
      algorithmContext.copy(
        namespace = "ai.h2o.sparkling.ml.models",
        entityName = algorithmContext.entityName + "MOJOModel")
    }

    for ((mojoContext, parameterContext) <- mojoConfiguration.zip(parametersConfiguration)) {
      val content = mojoTemplates(languageExtension)(mojoContext, parameterContext)
      writeResultToFile(content, mojoContext, languageExtension, destinationDir)
    }

    val mojoFactoryContext = mojoConfiguration.head.copy(entityName = "H2OMOJOModelFactory")
    val content = mojoFactoryTemplates(languageExtension)(mojoConfiguration)
    writeResultToFile(content, mojoFactoryContext, languageExtension, destinationDir)
  }
}
