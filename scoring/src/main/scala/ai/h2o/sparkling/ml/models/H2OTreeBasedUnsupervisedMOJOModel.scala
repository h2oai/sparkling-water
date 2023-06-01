package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.H2OTreeBasedMOJOParams
import hex.genmodel.MojoModel
import hex.genmodel.algos.tree.SharedTreeMojoModel

class H2OTreeBasedUnsupervisedMOJOModel(override val uid: String)
  extends H2OUnsupervisedMOJOModel(uid)
  with H2OTreeBasedMOJOParams {

  override private[sparkling] def setSpecificParams(mojoModel: MojoModel): Unit = {
    super.setSpecificParams(mojoModel)
    mojoModel match {
      case treeModel: SharedTreeMojoModel =>
        set(ntrees -> treeModel.getNTreeGroups() * treeModel.getNTreesPerGroup())
      case unexpectedModel =>
        val algorithmName = unexpectedModel._modelDescriptor.algoFullName()
        logError(s"Tried to read tree-based properties from MOJO model of $algorithmName.")
    }
  }
}

object H2OTreeBasedUnsupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OTreeBasedUnsupervisedMOJOModel]
