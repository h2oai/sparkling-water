package ai.h2o.sparkling.ml.algos

import hex.genmodel.utils.DistributionFamily
import hex.glm.GLMModel.GLMParameters.Family

object ProblemType extends Enumeration {
  type ProblemType = Value
  val Classification, Regression, Both = Value

  def familyToProblemType(family: String): ProblemType = {
    val enumValue = Family.valueOf(family)
    enumValue match {
      case Family.gaussian => Regression
      case Family.binomial => Classification
      case Family.fractionalbinomial => Regression
      case Family.ordinal => Classification
      case Family.quasibinomial => Regression
      case Family.multinomial => Classification
      case Family.poisson => Regression
      case Family.gamma => Regression
      case Family.tweedie => Regression
    }
  }

  def distributionToProblemType(distribution: String): ProblemType = {
    val enumValue = DistributionFamily.valueOf(distribution)
    enumValue match {
      case DistributionFamily.AUTO => Both
      case DistributionFamily.bernoulli => Classification
      case DistributionFamily.quasibinomial => Regression
      case DistributionFamily.multinomial => Classification
      case DistributionFamily.gaussian => Regression
      case DistributionFamily.poisson => Regression
      case DistributionFamily.gamma => Regression
      case DistributionFamily.laplace => Regression
      case DistributionFamily.quantile => Regression
      case DistributionFamily.huber => Regression
      case DistributionFamily.tweedie => Regression
      case DistributionFamily.custom => Both
    }
  }
}
