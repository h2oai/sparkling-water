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

package hex.schemas

import SVMV3.SVMParametersV3
import hex.schemas.SVMModelV3.SVMModelOutputV3
import org.apache.spark.ml.spark.models.svm.{SVMModel, SVMParameters}
import water.api.schemas3.{ModelOutputSchemaV3, ModelSchemaV3}
import water.api.API

class SVMModelV3 extends ModelSchemaV3[SVMModel,
  SVMModelV3,
  SVMParameters,
  SVMParametersV3,
  SVMModel.SVMOutput,
  SVMModelV3.SVMModelOutputV3] {

  override def createParametersSchema(): SVMParametersV3 = { new SVMParametersV3() }
  override def createOutputSchema(): SVMModelOutputV3 = { new SVMModelOutputV3() }
  
}

object SVMModelV3 {

  final class SVMModelOutputV3 extends ModelOutputSchemaV3[SVMModel.SVMOutput, SVMModelOutputV3] {
    // Output fields
    @API(help = "Iterations executed") var iterations: Int = 0
    @API(help = "Interceptor") var interceptor: Double = 0
    @API(help = "Weights") var weights: Array[Double] = Array()
  }

}
  
