package ai.h2o.sparkling.api.generation.common

/**
  * It's expected that this configuration source describes just one algorithm (e.g. AutoML, GridSearch, ...)
  * where the whole parametersConfiguration (sequence) is relevant to the algorithm
  */
trait SingleAlgorithmConfiguration extends ConfigurationSource {

  override def algorithmParametersPairs: Seq[(AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {
    Seq((algorithmConfiguration.head, parametersConfiguration))
  }

  override def specificAlgorithmParametersPairs
      : Seq[(ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {

    val algos = problemSpecificAlgorithmConfiguration

    if (algos.isEmpty) Seq.empty else Seq((algos.head, parametersConfiguration))
  }
}
