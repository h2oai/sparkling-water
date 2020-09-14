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

import ai.h2o.sparkling.{H2OColumnType, H2OFrame}
import hex.kmeans.KMeansModel.KMeansParameters

private[algos] trait H2OKMeansExtras extends H2OAlgorithm[KMeansParameters] {

  def getFoldCol(): String

  def setFoldCol(value: String): this.type

  override protected def prepareH2OTrainFrameForFitting(trainFrame: H2OFrame): Unit = {
    super.prepareH2OTrainFrameForFitting(trainFrame)
    val stringCols = trainFrame.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    if (stringCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Following columns are of type string: '${stringCols.mkString(", ")}', but" +
          " H2OKMeans does not accept string columns. However, you can use the 'columnsToCategorical' methods on H2OKMeans." +
          " These methods ensure that string columns are converted to representation H2O-3 understands.")
    }
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getFoldCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
