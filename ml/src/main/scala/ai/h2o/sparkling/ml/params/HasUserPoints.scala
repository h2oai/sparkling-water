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

package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.H2OContext

trait HasUserPoints extends H2OAlgoParamsBase {
  private val userPoints = new NullableDoubleArrayArrayParam(
    this,
    "userPoints",
    "This option allows" +
      " you to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
      " points must have the same number of columns as the training observations. The number of rows must equal" +
      " the number of clusters.")

  setDefault(userPoints -> null)

  def getUserPoints(): Array[Array[Double]] = $(userPoints)

  def setUserPoints(value: Array[Array[Double]]): this.type = set(userPoints, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++ Map("user_points" -> getUserPointAsH2OFrameKeyString())
  }

  private def getUserPointAsH2OFrameKeyString(): String = {
    val userPoints = getUserPoints()
    if (userPoints == null) {
      null
    } else {
      val spark = SparkSessionUtils.active
      import spark.implicits._
      val df = spark.sparkContext.parallelize(userPoints).toDF()
      val hc = H2OContext.ensure(
        "H2OContext needs to be created in order to train the H2OKMeans model. " +
          "Please create one as H2OContext.getOrCreate().")
      hc.asH2OFrame(df).frameId
    }
  }
}
