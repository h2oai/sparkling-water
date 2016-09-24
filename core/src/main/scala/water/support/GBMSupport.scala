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

import hex.genmodel.utils.DistributionFamily
import hex.tree.gbm.GBMModel
import org.apache.spark.h2o._

trait GBMSupport {

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


// Create companion object
object GBMSupport extends GBMSupport

