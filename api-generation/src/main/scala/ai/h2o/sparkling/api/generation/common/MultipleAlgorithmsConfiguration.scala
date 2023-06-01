package ai.h2o.sparkling.api.generation.common

/**
  * It's expected that this configuration source describes several (simple) algorithms
  * where each algorithm has just one relevant parameter in parametersConfiguration (sequence)
  */
trait MultipleAlgorithmsConfiguration extends ConfigurationSource {

  override def algorithmParametersPairs: Seq[(AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {
    algorithmConfiguration.zip(parametersConfiguration).map { case (alg, par) => (alg, Seq(par)) }
  }

  override def specificAlgorithmParametersPairs
      : Seq[(ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {

    problemSpecificAlgorithmConfiguration.zip(parametersConfiguration).map { case (alg, par) => (alg, Seq(par)) }
  }
}
