/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
