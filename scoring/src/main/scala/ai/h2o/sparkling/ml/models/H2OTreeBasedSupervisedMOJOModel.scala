package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.params.{H2OSupervisedMOJOParams, H2OTreeBasedMOJOParams}
import hex.genmodel.MojoModel
import hex.genmodel.algos.tree.SharedTreeMojoModel
import hex.genmodel.algos.xgboost.XGBoostMojoModel

class H2OTreeBasedSupervisedMOJOModel(override val uid: String)
  extends H2OSupervisedMOJOModel(uid)
  with H2OTreeBasedMOJOParams
  with H2OSupervisedMOJOParams {

  override private[sparkling] def setSpecificParams(mojoModel: MojoModel): Unit = {
    super.setSpecificParams(mojoModel)
    mojoModel match {
      case treeModel: SharedTreeMojoModel =>
        set(ntrees -> treeModel.getNTreeGroups() * treeModel.getNTreesPerGroup())
      case xgBoostModel: XGBoostMojoModel =>
        set(ntrees -> xgBoostModel._ntrees)
      case unexpectedModel =>
        val algorithmName = unexpectedModel._modelDescriptor.algoFullName()
        logError(s"Tried to read tree-based properties from MOJO model of $algorithmName.")
    }
  }
}

object H2OTreeBasedSupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OTreeBasedSupervisedMOJOModel]
