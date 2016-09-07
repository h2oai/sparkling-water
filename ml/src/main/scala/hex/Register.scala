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
import water.H2O
import water.api.{GridSearchHandler, ModelBuilderHandler, RestApi}

class Register extends RestApi {

  override def register(hc: H2OContext) = {

    val models = Seq(new SVM(true, hc))

    for (algo <- models) {
      val base: String = algo.getClass.getSimpleName
      val lbase: String = base.toLowerCase
      val bh_clz = classOf[ModelBuilderHandler[_, _, _]]
      val version: Int = 3
      H2O.register(
        "POST /" + version + "/ModelBuilders/" + lbase,
        bh_clz,
        "train",
        "train_" + lbase,
        "Train a " + base + " model."
      )
      H2O.register(
        "POST /" + version + "/ModelBuilders/" + lbase + "/parameters",
        bh_clz,
        "validate_parameters",
        "validate_" + lbase,
        "Validate a set of " + base + " model builder parameters."
      )
      H2O.register(
        "POST /99/Grid/" + lbase,
        classOf[GridSearchHandler[_,_,_,_]],
        "train",
        "grid_search_" + lbase,
        "Run grid search for " + base + " model."
      )
    }
  }
}
