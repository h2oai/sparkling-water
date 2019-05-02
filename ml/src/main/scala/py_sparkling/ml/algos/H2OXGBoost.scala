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
package py_sparkling.ml.algos

import hex.tree.xgboost.{XGBoost, XGBoostModel}
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import org.apache.spark.ml.h2o.algos.H2OAlgorithmReader
import org.apache.spark.ml.util.{MLReadable, MLReader}

/**
  * H2O XGBoost Wrapper for PySparkling
  */
class H2OXGBoost(override val uid: String) extends org.apache.spark.ml.h2o.algos.H2OXGBoost(uid) {

  override def trainModel(params: XGBoostParameters): XGBoostModel = new XGBoost(params).trainModel().get()
}

private[algos] object H2OXGBoost extends MLReadable[H2OXGBoost] {

  private final val defaultFileName = "xgboost_params"

  override def read: MLReader[H2OXGBoost] = H2OAlgorithmReader.create[H2OXGBoost](defaultFileName)

  override def load(path: String): H2OXGBoost = super.load(path)
}
