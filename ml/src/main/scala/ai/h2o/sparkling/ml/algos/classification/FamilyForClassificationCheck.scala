package ai.h2o.sparkling.ml.algos.classification

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.{H2OAlgoCommonUtils, ProblemType}
import org.apache.spark.sql.Dataset

trait FamilyForClassificationCheck extends H2OAlgoCommonUtils {
  def getFamily(): String

  override private[sparkling] def prepareDatasetForFitting(dataset: Dataset[_]): (H2OFrame, Option[H2OFrame]) = {
    val family = getFamily()
    val problemType = ProblemType.familyToProblemType(family)
    if (problemType != ProblemType.Both && problemType != ProblemType.Classification) {
      throw new RuntimeException(s"Family '$family' is not supported for a classification problem.")
    }
    super.prepareDatasetForFitting(dataset)
  }
}
