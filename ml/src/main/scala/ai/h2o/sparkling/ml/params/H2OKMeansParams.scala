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

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.utils.SparkSessionUtils
import hex.kmeans.KMeans
import hex.kmeans.KMeansModel.KMeansParameters
import hex.schemas.GLMV3.GLMParametersV3

trait H2OKMeansParams extends H2OAlgoUnsupervisedParams[KMeansParameters] with HasUserPoints {

  type H2O_SCHEMA = GLMParametersV3

  protected def paramTag = reflect.classTag[KMeansParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val maxIterations = intParam("maxIterations", "Maximum number of KMeans iterations to find the centroids.")
  private val standardize =
    booleanParam("standardize", "Standardize the numeric columns to have a mean of zero and unit variance.")
  private val init = stringParam("init", "Initialization mode for finding the initial cluster centers.")
  private val estimateK = booleanParam(
    "estimateK",
    "If enabled, the algorithm tries to identify optimal number of clusters, up to k clusters.")
  private val k = intParam("k", "Number of clusters to generate.")

  //
  // Default values
  //
  setDefault(
    maxIterations -> 10,
    standardize -> true,
    init -> KMeans.Initialization.Furthest.name(),
    userPoints -> null,
    estimateK -> false,
    k -> 2)

  //
  // Getters
  //
  def getMaxIterations(): Int = $(maxIterations)

  def getStandardize(): Boolean = $(standardize)

  def getInit(): String = $(init)

  def getEstimateK(): Boolean = $(estimateK)

  def getK(): Int = $(k)

  //
  // Setters
  //
  def setMaxIterations(value: Int): this.type = set(maxIterations, value)

  def setStandardize(value: Boolean): this.type = set(standardize, value)

  def setInit(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[KMeans.Initialization](value)
    set(init, validated)
  }

  def setEstimateK(value: Boolean): this.type = set(estimateK, value)

  def setK(value: Int): this.type = set(k, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "max_iterations" -> getMaxIterations(),
        "standardize" -> getStandardize(),
        "init" -> getInit(),
        "user_points" -> getUserPointAsH2OFrameKeyString(),
        "estimate_k" -> getEstimateK(),
        "k" -> getK())
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
