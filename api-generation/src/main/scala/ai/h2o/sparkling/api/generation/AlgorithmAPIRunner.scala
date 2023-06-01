package ai.h2o.sparkling.api.generation

import ai.h2o.sparkling.api.generation.common.{APIRunnerBase, AlgorithmConfigurations, AutoMLConfiguration, ConfigurationSource, FeatureEstimatorConfigurations, GridSearchConfiguration, StackedEnsembleConfiguration, SubstitutionContextBase, Word2VecConfiguration}
import ai.h2o.sparkling.api.generation.python.Word2VecTemplate

object AlgorithmAPIRunner extends APIRunnerBase {

  private val algorithmTemplates = Map("scala" -> scala.AlgorithmTemplate, "py" -> python.AlgorithmTemplate)

  private val problemSpecificAlgorithmTemplates =
    Map("scala" -> scala.ProblemSpecificAlgorithmTemplate, "py" -> python.ProblemSpecificAlgorithmTemplate)

  private val parameterTemplates = Map("scala" -> scala.ParametersTemplate, "py" -> python.ParametersTemplate)
  private val skippedScalaAlgorithms = Seq("H2OAutoML", "H2OGridSearch", "H2OStackedEnsemble")

  def main(args: Array[String]): Unit = {
    val languageExtension = args(0)
    val destinationDir = args(1)

    generateWord2Vec(languageExtension, destinationDir)

    val configurationSources: Seq[ConfigurationSource] = Seq(
      new AlgorithmConfigurations(),
      new FeatureEstimatorConfigurations(),
      new AutoMLConfiguration(),
      new GridSearchConfiguration(),
      new StackedEnsembleConfiguration())

    for (source <- configurationSources) {

      for (substitutionContext <- source.parametersConfiguration) {
        val content = parameterTemplates(languageExtension)(substitutionContext)
        writeResultToFile(content, substitutionContext, languageExtension, destinationDir)
      }

      for ((algorithmContext, parameterContext) <- source.algorithmParametersPairs
           if shouldGenerateAlgorithm(languageExtension, algorithmContext.entityName)) {
        val content = algorithmTemplates(languageExtension)(algorithmContext, parameterContext)
        writeResultToFile(content, algorithmContext, languageExtension, destinationDir)
      }

      val specificAlgorithmCombinations = source.specificAlgorithmParametersPairs

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
    }
  }

  private def shouldGenerateAlgorithm(language: String, entityName: String): Boolean = {
    if (language == "scala" && skippedScalaAlgorithms.contains(entityName)) false
    else true
  }

  private def generateWord2Vec(languageExtension: String, destinationDir: String): Unit = {
    val w2vContext = new Word2VecConfiguration().word2VecParametersSubstitutionContext
    val content = parameterTemplates(languageExtension)(w2vContext)
    writeResultToFile(content, w2vContext, languageExtension, destinationDir)

    if (languageExtension != "scala") {
      val content = Word2VecTemplate.apply()
      val context = new SubstitutionContextBase {
        override def namespace: String = "ai.h2o.sparkling.ml.features"

        override def entityName: String = "H2OWord2Vec"
      }
      writeResultToFile(content, context, languageExtension, destinationDir)
    }
  }

}
