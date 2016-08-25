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

import hex.schemas.GaussianMixtureModelV3.GaussianMixtureModelOutputV3
import hex.schemas.GaussianMixtureV3.GaussianMixtureParametersV3
import org.apache.spark.ml.spark.models.gm.{ClusteringUtils, GaussianMixtureModel, GaussianMixtureParameters}
import org.apache.spark.ml.spark.models.gm.GaussianMixtureModel.GaussianMixtureOutput
import water.api.API
import water.api.schemas3.{ModelOutputSchemaV3, ModelSchemaV3, TwoDimTableV3}

class GaussianMixtureModelV3 extends ModelSchemaV3[GaussianMixtureModel,
  GaussianMixtureModelV3,
  GaussianMixtureParameters,
  GaussianMixtureParametersV3,
  GaussianMixtureOutput,
  GaussianMixtureModelOutputV3] {

  override def createParametersSchema(): GaussianMixtureParametersV3 = new GaussianMixtureParametersV3

  override def createOutputSchema(): GaussianMixtureModelOutputV3 = new GaussianMixtureModelOutputV3

}

object GaussianMixtureModelV3 {
  final class GaussianMixtureModelOutputV3 extends ModelOutputSchemaV3[GaussianMixtureOutput, GaussianMixtureModelOutputV3] {
    @API(help = "Cluster Centers[k][features]")
    var centers: TwoDimTableV3 = _

    @API(help = "Cluster Centers[k][features] on Standardized Data")
    var centers_std: TwoDimTableV3 = _

    override def fillFromImpl(impl: GaussianMixtureOutput): GaussianMixtureModelOutputV3 = {
      val gmv3: GaussianMixtureModelOutputV3 = super.fillFromImpl(impl)
      gmv3.centers = new TwoDimTableV3().fillFromImpl(ClusteringUtils.createCenterTable(impl, false))
      if (impl._centers_std_raw != null) {
        gmv3.centers_std = new TwoDimTableV3().fillFromImpl(ClusteringUtils.createCenterTable(impl, true))
      }
      gmv3
    }
  }
}
