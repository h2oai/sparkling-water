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

import hex.{Model, ModelMetrics}
import water.fvec.Frame

/**
  * Interface to access different model metrics provided by H2O model.
  */
trait ModelMetricsSupport {

  /* Helper class to have nice API */
  class ModelMetricsExtractor[T <: ModelMetrics] {
    def apply[M <: Model[M, P, O], P <: hex.Model.Parameters, O <: hex.Model.Output]
    (model: Model[M, P, O], fr: Frame): T = {
      // Fetch model metrics and rescore if it is necessary
      if (ModelMetrics.getFromDKV(model, fr).asInstanceOf[T] == null) {
        model.score(fr).delete()
      }
      ModelMetrics.getFromDKV(model, fr).asInstanceOf[T]
    }
  }

  def modelMetrics[T <: ModelMetrics] = new ModelMetricsExtractor[T]
}

// Create companion object
object ModelMetricsSupport extends ModelMetricsSupport
