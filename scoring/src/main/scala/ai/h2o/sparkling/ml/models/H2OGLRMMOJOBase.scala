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

import ai.h2o.sparkling.ml.params.H2OGLRMExtraParams
import hex.genmodel.easy.EasyPredictModelWrapper

trait H2OGLRMMOJOBase extends H2ODimReductionMOJOModel with H2OGLRMExtraParams {

  protected override def reconstructedEnabled: Boolean = getWithReconstructedCol()

  private[sparkling] override def setEasyPredictModelWrapperConfiguration(
      config: EasyPredictModelWrapper.Config): EasyPredictModelWrapper.Config = {
    super.setEasyPredictModelWrapperConfiguration(config)
    config.setGLRMIterNumber(getMaxScoringIterations())
    config.setEnableGLRMReconstrut(getWithReconstructedCol())
    config
  }
}