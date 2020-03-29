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
package water.support

import ai.h2o.sparkling.macros.DeprecatedMethod
import hex.genmodel.utils.DistributionFamily
import hex.tree.gbm.GBMModel
import org.apache.spark.expose.Logging
import org.apache.spark.h2o._

/**
 * Support class to create and train GBM
 */
@Deprecated
trait GBMSupport extends Logging {

  /**
   * Create Gradient Boosting Model for the basic usage. This method exposes
   * the basic configuration of the model. If you need to specify some arguments which are not exposed, please
   * use the DeepLearning model via Sparkling Water pipelines API or using the raw java API
   *
   * @param train    frame to train
   * @param test     test frame
   * @param response response column
   * @param modelId  name of the model
   * @param ntrees   number of trees
   * @param depth    depth
   * @param family   distribution family
   * @tparam T H2O Frame Type
   * @return Gradient Boosting Model
   */
  @DeprecatedMethod("ai.h2o.sparkling.algos.H2OGBM", "3.32")
  def GBMModel[T <: Frame](train: T, test: T, response: String,
                           modelId: String = "model",
                           ntrees: Int = 50,
                           depth: Int = 6,
                           family: DistributionFamily = DistributionFamily.AUTO): GBMModel = {
    import hex.tree.gbm.GBM
    import hex.tree.gbm.GBMModel.GBMParameters

    val gbmParams = new GBMParameters()
    gbmParams._train = train._key
    gbmParams._valid = if (test != null) {
      test._key
    } else {
      null
    }
    gbmParams._response_column = response
    gbmParams._ntrees = ntrees
    gbmParams._max_depth = depth
    gbmParams._distribution = family

    val gbm = new GBM(gbmParams, water.Key.make(modelId))
    val model = gbm.trainModel.get
    model
  }
}

@Deprecated
object GBMSupport extends GBMSupport

