package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{H2OColumnType, H2OFrame}

trait DistributionBasedH2OTrainFramePreparation extends H2OTrainFramePreparation {

  def getDistribution(): String

  def getLabelCol(): String

  override protected def prepareH2OTrainFrameForFitting(trainFrame: H2OFrame): Unit = {
    super.prepareH2OTrainFrameForFitting(trainFrame)
    if (ProblemType.distributionToProblemType(getDistribution()) == ProblemType.Classification) {
      if (trainFrame.columns.find(_.name == getLabelCol()).get.dataType != H2OColumnType.`enum`) {
        trainFrame.convertColumnsToCategorical(Array(getLabelCol()))
      }
    }
  }
}
