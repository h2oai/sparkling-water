package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OGLRMExtraParams
import hex.genmodel.easy.EasyPredictModelWrapper

trait H2OGLRMMOJOBase extends H2ODimReductionMOJOModel with H2OGLRMExtraParams {

  protected override def reconstructedEnabled: Boolean = getWithReconstructedCol()

  private[sparkling] override def getEasyPredictModelWrapperConfigurationInitializers()
      : Seq[EasyPredictModelWrapperConfigurationInitializer] = {
    val superInitializers = super.getEasyPredictModelWrapperConfigurationInitializers()
    val maxScoringIterations = getMaxScoringIterations()
    val enableGLRMReconstruct = getWithReconstructedCol()

    superInitializers ++ Seq[EasyPredictModelWrapperConfigurationInitializer](
      _.setGLRMIterNumber(maxScoringIterations),
      _.setEnableGLRMReconstrut(enableGLRMReconstruct))
  }
}
