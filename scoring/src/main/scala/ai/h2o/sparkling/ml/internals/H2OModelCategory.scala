package ai.h2o.sparkling.ml.internals

/**
  * Copied from H2O's class ModelCategory
  */
private[sparkling] object H2OModelCategory extends Enumeration {
  val Unknown, Binomial, Multinomial, Ordinal, Regression, HGLMRegression, Clustering, AutoEncoder, TargetEncoder,
      DimReduction, WordEmbedding, CoxPH, AnomalyDetection = Value

  def fromString(modelCategory: String): Value = {
    values
      .find(_.toString == modelCategory)
      .getOrElse(throw new RuntimeException(s"Unknown model category $modelCategory"))
  }
}
