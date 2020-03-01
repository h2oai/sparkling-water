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
package ai.h2o.sparkling.ml.params

import hex.Model.Parameters

/**
  * A trait extracting a shared parameters among all supervised simple algorithms (all except Grid & AutoML).
  */
trait H2OAlgoSupervisedParams[P <: Parameters] extends H2OAlgoParamsHelper[P]
  with H2OCommonSupervisedParams with H2OAlgoCommonParams[P] {

  override protected def updateH2OParamsREST(): Map[String, Any] = {
    super.updateH2OParamsREST() ++
      Map(
        "response_column" -> getLabelCol(),
        "offset_column" -> getOffsetCol()
      )
  }

  /** Update H2O params based on provided parameters to Spark Transformer/Estimator */
  override protected def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._response_column = getLabelCol()
    parameters._offset_column = getOffsetCol()
  }
}
