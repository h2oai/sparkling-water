package ai.h2o.sparkling.api.generation.common

trait ConfigurationSource {

  def algorithmConfiguration: Seq[AlgorithmSubstitutionContext] = Seq.empty

  def problemSpecificAlgorithmConfiguration: Seq[ProblemSpecificAlgorithmSubstitutionContext] = Seq.empty

  def parametersConfiguration: Seq[ParameterSubstitutionContext] = Seq.empty

  def modelOutputConfiguration: Seq[ModelOutputSubstitutionContext] = Seq.empty

  def algorithmParametersPairs: Seq[(AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = Seq.empty

  def specificAlgorithmParametersPairs
      : Seq[(ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = Seq.empty
}
