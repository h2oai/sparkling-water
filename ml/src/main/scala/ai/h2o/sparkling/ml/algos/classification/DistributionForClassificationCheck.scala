package ai.h2o.sparkling.ml.algos.classification

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.{H2OAlgoCommonUtils, ProblemType}
import org.apache.spark.sql.Dataset

trait DistributionForClassificationCheck extends H2OAlgoCommonUtils {
  def getDistribution(): String

  override private[sparkling] def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame]) = {
    val distribution = getDistribution()
    val problemType = ProblemType.distributionToProblemType(distribution)
    if (problemType != ProblemType.Both && problemType != ProblemType.Classification) {
      throw new RuntimeException(s"Distribution '$distribution' is not supported for a classification problem.")
    }
    super.prepareDatasetForFitting(dataset)
  }
}
