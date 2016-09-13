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
    @API(help = "Distribution weights.")
    var weightsTable: TwoDimTableV3 = _

    @API(help = "Distribution means.")
    var muTable: TwoDimTableV3 = _

    @API(help = "Distribution variances.")
    var sigmaTable: TwoDimTableV3 = _

    private def generateNames(prefix: String, length: Int): Array[String] = (0 until length).map(prefix+_).toArray

    override def fillFromImpl(impl: GaussianMixtureOutput): GaussianMixtureModelOutputV3 = {
      val gmv3: GaussianMixtureModelOutputV3 = super.fillFromImpl(impl)
      gmv3.weightsTable = new TwoDimTableV3().fillFromImpl(
        ClusteringUtils.create2DTable(
          impl,
          Array[Array[Double]](impl._weights),
          "Weight",
          generateNames("W", impl._weights.length),
          "Distribution weights")
      )

      gmv3.muTable = new TwoDimTableV3().fillFromImpl(
        ClusteringUtils.create2DTable(
          impl,
          impl._mu,
          "Mean",
          generateNames("M", if(impl._mu.isEmpty) 0 else impl._mu.head.length),
          "Distribution means")
      )

      gmv3.sigmaTable = new TwoDimTableV3().fillFromImpl(
        ClusteringUtils.create2DTable(
          impl,
          impl._sigma,
          "Variance",
          generateNames("V", if(impl._sigma.isEmpty) 0 else impl._sigma.head.length),
          "Distribution variances")
      )
      gmv3
    }
  }
}
