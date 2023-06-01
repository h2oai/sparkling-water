package ai.h2o.sparkling.ml.models

import hex.genmodel.MojoModel

trait SpecificMOJOParameters {
  private[sparkling] def setSpecificParams(mojoModel: MojoModel): Unit = {}
}
