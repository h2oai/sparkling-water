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

import ai.h2o.sparkling.ml.params.H2OKMeansParams
import ai.h2o.sparkling.{H2OColumnType, H2OFrame}
import hex.kmeans.KMeansModel.KMeansParameters
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}

/**
 * H2O KMeans algorithm exposed via Spark ML pipelines.
 */
class H2OKMeans(override val uid: String) extends H2OUnsupervisedAlgorithm[KMeansParameters] with H2OKMeansParams {

  override protected def prepareH2OTrainFrameForFitting(trainFrame: H2OFrame): Unit = {
    super.prepareH2OTrainFrameForFitting(trainFrame)
    val stringCols = trainFrame.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    if (stringCols.nonEmpty) {
      throw new IllegalArgumentException(s"Following columns are of type string: '${stringCols.mkString(", ")}', but" +
        s" H2OKMeans does not accept string columns. However, you can use the `allStringColumnsToCategorical`" +
        s" or 'columnsToCategorical' methods on H2OKMeans. These methods ensure that string columns are " +
        s" converted to representation H2O-3 understands.")
    }
  }

  def this() = this(Identifiable.randomUID(classOf[H2OKMeans].getSimpleName))
}

object H2OKMeans extends DefaultParamsReadable[H2OKMeans]

