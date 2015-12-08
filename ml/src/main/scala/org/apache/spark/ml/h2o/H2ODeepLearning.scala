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

package org.apache.spark.ml.h2o

import hex.deeplearning.{DeepLearning, DeepLearningModel, DeepLearningParameters}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.param.{ParamMap, Param, Params}
import org.apache.spark.ml.{Estimator, PredictionModel}
import org.apache.spark.mllib
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Deep learning ML component.
 */
class H2ODeepLearningModel(model: DeepLearningModel)
  extends PredictionModel[mllib.linalg.Vector, H2ODeepLearningModel] {

  override protected def predict(features: mllib.linalg.Vector): Double = ???

  override val uid: String = "dlModel"

  override def copy(extra: ParamMap): H2ODeepLearningModel = ???
}

class H2ODeepLearning()(implicit hc: H2OContext)
  extends Estimator[H2ODeepLearningModel] with HasDeepLearningParams {

  override def fit(dataset: DataFrame): H2ODeepLearningModel = {
    import hc._
    val params = new DeepLearningParameters
    params._train = dataset
    val model = new DeepLearning(params).trainModel().get()
    params._train.remove()
    val dlm = new H2ODeepLearningModel(model)
    dlm
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = "dl"

  override def copy(extra: ParamMap): Estimator[H2ODeepLearningModel] = ???
}

trait HasDeepLearningParams extends Params {
  val deepLearningParams: Param[DeepLearningParameters] = new Param[DeepLearningParameters]("DL params",
    "deepLearningParams", "H2O's DeepLearning parameters", (t:DeepLearningParameters) => true)

  def getDeepLearningParams: DeepLearningParameters = get(deepLearningParams).get

}

