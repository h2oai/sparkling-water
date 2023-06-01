package ai.h2o.sparkling.api.generation

import ai.h2o.sparkling.api.generation.common._

object MOJOModelAPIRunner extends APIRunnerBase with MetricsConfigurations {

  private val mojoTemplates = Map("scala" -> scala.MOJOModelTemplate, "py" -> python.MOJOModelTemplate)
  private val mojoFactoryTemplates =
    Map("scala" -> scala.MOJOModelFactoryTemplate, "py" -> python.MOJOModelFactoryTemplate)
  private val metricsTemplates =
    Map("scala" -> scala.ModelMetricsTemplate, "py" -> python.ModelMetricsTemplate, "R" -> r.ModelMetricsTemplate)
  private val metricsBaseTemplates =
    Map("py" -> python.MetricsFactoryTemplate, "R" -> r.MetricsFactoryTemplate)

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    val configurationSources: Seq[ConfigurationSource] =
      Seq(new AlgorithmConfigurations(), new FeatureEstimatorConfigurations(), new StackedEnsembleConfiguration())

    var allMojos: Seq[AlgorithmSubstitutionContext] = Seq.empty

    for (source <- configurationSources) {

      if (languageExtension != "R") {
        val mojoConfiguration = source.algorithmConfiguration.map { algorithmContext =>
          algorithmContext.copy(
            namespace = "ai.h2o.sparkling.ml.models",
            entityName = algorithmContext.entityName + "MOJOModel")
        }

        allMojos ++= mojoConfiguration

        for (((mojoContext, parameterContext), modelOutputContext) <- mojoConfiguration
               .zip(source.parametersConfiguration)
               .zip(source.modelOutputConfiguration)) {
          val content = mojoTemplates(languageExtension)(mojoContext, parameterContext, modelOutputContext)
          writeResultToFile(content, mojoContext, languageExtension, destinationDir)
        }

        val entityName = if (languageExtension == "py") "H2OMOJOModel" else "H2OMOJOModelFactory"

        val mojoFactoryContext = allMojos.head.copy(entityName = entityName)
        val content = mojoFactoryTemplates(languageExtension)(allMojos)
        writeResultToFile(content, mojoFactoryContext, languageExtension, destinationDir)
      }
    }

    for (metricsContext <- metricsConfiguration) {
      val content = metricsTemplates(languageExtension)(metricsContext)
      writeResultToFile(content, metricsContext, languageExtension, destinationDir)
    }

    if (languageExtension != "scala") {
      val metricsFactoryContext = metricsConfiguration.head.copy(entityName = "H2OMetricsFactory")
      val content = metricsBaseTemplates(languageExtension)(metricsConfiguration)
      writeResultToFile(content, metricsFactoryContext, languageExtension, destinationDir)

      if (languageExtension == "py") {
        val metricsInitContext = metricsConfiguration.head.copy(entityName = "__init__")
        val initContent = python.MetricsInitTemplate(metricsConfiguration :+ metricsFactoryContext)
        writeResultToFile(initContent, metricsInitContext, languageExtension, destinationDir)
      }
    }
  }
}
