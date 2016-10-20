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

import hex.schemas.ALSModelV3.ALSModelOutputV3
import hex.schemas.ALSV3.ALSParametersV3
import org.apache.spark.ml.spark.models.als.{ALSModel, ALSParameters}
import water.api.API
import water.api.schemas3.{ModelOutputSchemaV3, ModelSchemaV3}

class ALSModelV3 extends ModelSchemaV3[ALSModel, ALSModelV3,
  ALSParameters,
  ALSParametersV3,
  ALSModel.ALSOutput,
  ALSModelV3.ALSModelOutputV3] {

  override def createParametersSchema(): ALSParametersV3 = { new ALSParametersV3() }
  override def createOutputSchema(): ALSModelOutputV3 = { new ALSModelOutputV3() }

}

object ALSModelV3 {
  final class ALSModelOutputV3 extends ModelOutputSchemaV3[ALSModel.ALSOutput, ALSModelOutputV3] {
    // Output fields
    @API(help = "Iterations executed") var iterations: Int = 0
    @API(help = "Rank") var rank: Int = 0
    // TODO add somehow the user and product features
  }
}
