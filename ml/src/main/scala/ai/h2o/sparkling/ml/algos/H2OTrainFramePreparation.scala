package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.H2OFrame

trait H2OTrainFramePreparation {
  protected def prepareH2OTrainFrameForFitting(frame: H2OFrame): Unit = {}
}
