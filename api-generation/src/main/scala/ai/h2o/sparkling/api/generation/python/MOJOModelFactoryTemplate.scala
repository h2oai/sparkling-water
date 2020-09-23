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

package ai.h2o.sparkling.api.generation.python

import ai.h2o.sparkling.api.generation.common.{AlgorithmSubstitutionContext, EntitySubstitutionContext}

object MOJOModelFactoryTemplate extends ((Seq[AlgorithmSubstitutionContext]) => String) with PythonEntityTemplate {

  def apply(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {
    val specificMojoModels = mojoSubstitutionContexts.map(_.entityName)
    val imports = Seq(
      "pyspark.ml.util._jvm",
      "ai.h2o.sparkling.Initializer.Initializer",
      "ai.h2o.sparkling.ml.models.H2OMOJOSettings.H2OMOJOSettings",
      "ai.h2o.sparkling.ml.params.H2OMOJOModelParams.H2OTreeBasedSupervisedMOJOModelParams",
      "ai.h2o.sparkling.ml.params.H2OMOJOModelParams.H2OTreeBasedUnsupervisedMOJOModelParams",
      "ai.h2o.sparkling.ml.params.H2OMOJOModelParams.H2OSupervisedMOJOModelParams",
      "ai.h2o.sparkling.ml.params.H2OMOJOModelParams.H2OUnsupervisedMOJOModelParams",
      "ai.h2o.sparkling.ml.params.H2OMOJOModelParams.H2OMOJOModelParams") ++
      specificMojoModels.map(mojo => s"ai.h2o.sparkling.ml.models.$mojo.$mojo")

    val entitySubstitutionContext = EntitySubstitutionContext(
      mojoSubstitutionContexts.head.namespace,
      "H2OMOJOModelFactory",
      inheritedEntities = Seq.empty,
      imports)

    val factory = generateEntity(entitySubstitutionContext) {
      s"""    @staticmethod
         |    def createFromMojo(pathToMojo, settings=H2OMOJOSettings.default()):
         |        # We need to make sure that Sparkling Water classes are available on the Spark driver and executor paths
         |        Initializer.load_sparkling_jar()
         |        javaModel = _jvm().ai.h2o.sparkling.ml.models.H2OMOJOModel.createFromMojo(pathToMojo, settings.toJavaObject())
         |        return H2OMOJOModelFactory.createSpecificMOJOModel(javaModel)
         |
         |
         |    @staticmethod
         |    def createSpecificMOJOModel(javaModel):
         |        className = javaModel.getClass().getSimpleName()
         |        if className == "H2OTreeBasedSupervisedMOJOModel":
         |            return H2OTreeBasedSupervisedMOJOModel(javaModel)
         |        elif className == "H2OTreeBasedUnsupervisedMOJOModel":
         |            return H2OTreeBasedUnsupervisedMOJOModel(javaModel)
         |        elif className == "H2OSupervisedMOJOModel":
         |            return H2OSupervisedMOJOModel(javaModel)
         |        elif className == "H2OUnsupervisedMOJOModel":
         |            return H2OUnsupervisedMOJOModel(javaModel)
         |${generatePatternMatchingCases(mojoSubstitutionContexts)}
         |        else:
         |            return H2OMOJOModel(javaModel)
         |""".stripMargin
    }

    val models = s"""
       |class H2OMOJOModel(H2OMOJOModelParams, H2OMOJOModelFactory):
       |    pass
       |
       |
       |class H2OUnsupervisedMOJOModel(H2OUnsupervisedMOJOModelParams, H2OMOJOModelFactory):
       |    pass
       |
       |
       |class H2OSupervisedMOJOModel(H2OSupervisedMOJOModelParams, H2OMOJOModelFactory):
       |    pass
       |
       |
       |class H2OTreeBasedUnsupervisedMOJOModel(H2OTreeBasedUnsupervisedMOJOModelParams, H2OMOJOModelFactory):
       |    pass
       |
       |
       |class H2OTreeBasedSupervisedMOJOModel(H2OTreeBasedSupervisedMOJOModelParams, H2OMOJOModelFactory):
       |    pass
       |""".stripMargin

    factory + models
  }

  private def generatePatternMatchingCases(mojoSubstitutionContexts: Seq[AlgorithmSubstitutionContext]): String = {
    mojoSubstitutionContexts
      .map { mojoSubstitutionContext =>
        val modelName = mojoSubstitutionContext.entityName
        s"""        elif className == "$modelName":
           |            return $modelName(javaModel)""".stripMargin
      }
      .mkString("\n")
  }
}
