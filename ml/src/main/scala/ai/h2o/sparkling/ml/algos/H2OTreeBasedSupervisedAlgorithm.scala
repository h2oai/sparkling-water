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

package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OTreeBasedSupervisedMOJOModel
import hex.Model
import org.apache.spark.h2o.{H2OBaseModel, H2OBaseModelBuilder}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OTreeBasedSupervisedAlgorithm[
  B <: H2OBaseModelBuilder : ClassTag,
  M <: H2OBaseModel,
  P <: Model.Parameters : ClassTag] extends H2OSupervisedAlgorithm[B, M, P] {

  override def fit(dataset: Dataset[_]): H2OTreeBasedSupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OTreeBasedSupervisedMOJOModel]
  }
}
