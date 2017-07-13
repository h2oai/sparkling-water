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

package hex

import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.spark.models.svm.SVM
import water.api.{GridSearchHandler, ModelBuilderHandler, RequestServer, RestApi}

class SVMModelRestAPI extends RestApi {
  override def name: String = "SVM Model REST API"

  override def register(hc: H2OContext) = {

    val models = Seq(new SVM(true, hc))

    for (algo <- models) {
      val base: String = algo.getClass.getSimpleName
      val lbase: String = base.toLowerCase
      val bh_clz = classOf[ModelBuilderHandler[_, _, _]]
      val version: Int = 3

      RequestServer.registerEndpoint(
        "train_" + lbase,
        "POST /" + version + "/ModelBuilders/" + lbase,
        bh_clz,
        "train",
        "Train a " + base + " model.")

      RequestServer.registerEndpoint(
        "validate_" + lbase,
        "POST /" + version + "/ModelBuilders/" + lbase + "/parameters",
        bh_clz,
        "validate_parameters",
        "Validate a set of " + base + " model builder parameters."

      )

      RequestServer.registerEndpoint(
        "grid_search_" + lbase,
        "POST /99/Grid/" + lbase,
        classOf[GridSearchHandler[_, _, _, _]],
        "train",
        "Run grid search for " + base + " model."
      )
    }
  }
}
