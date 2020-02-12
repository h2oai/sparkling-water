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

import ai.h2o.sparkling.frame.{H2OColumnType, H2OFrame}
import ai.h2o.sparkling.ml.params.{H2OAlgoParamsHelper, H2OAlgoUnsupervisedParams}
import hex.kmeans.KMeansModel.KMeansParameters
import hex.kmeans.{KMeans, KMeansModel}
import hex.schemas.GLMV3.GLMParametersV3
import org.apache.spark.h2o.backends.external.RestApiUtils
import org.apache.spark.h2o.{Frame, H2OContext}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.SparkSession
import water.DKV

/**
  * H2O KMeans algorithm exposed via Spark ML pipelines.
  */
class H2OKMeans(override val uid: String) extends H2OUnsupervisedAlgorithm[KMeans, KMeansModel, KMeansParameters] with H2OKMeansParams {

  override protected def preProcessBeforeFit(trainFrameKey: String): Unit = {
    super.preProcessBeforeFit(trainFrameKey)
    val stringCols = if (RestApiUtils.isRestAPIBased()) {
      H2OFrame(trainFrameKey).columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    } else {
      val trainFrame = DKV.getGet[Frame](trainFrameKey)
      trainFrame.names.filter(name => trainFrame.vec(name).isString)
    }
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


/**
  * Parameters for Spark's API exposing underlying H2O model.
  */
trait H2OKMeansParams extends H2OAlgoUnsupervisedParams[KMeansParameters] {

  type H2O_SCHEMA = GLMParametersV3

  protected def paramTag = reflect.classTag[KMeansParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val maxIterations = intParam("maxIterations", "Maximum number of KMeans iterations to find the centroids.")
  private val standardize = booleanParam("standardize", "Standardize the numeric columns to have a mean of zero and unit variance.")
  private val init = stringParam("init", "Initialization mode for finding the initial cluster centers.")
  private val userPoints = nullableDoubleArrayArrayParam("userPoints", "This option allows" +
    " you to specify array of points, where each point represents coordinates of an initial cluster center. The user-specified" +
    " points must have the same number of columns as the training observations. The number of rows must equal" +
    " the number of clusters.")
  private val estimateK = booleanParam("estimateK", "If enabled, the algorithm tries to identify optimal number of clusters, up to k clusters.")
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
    k -> 2
  )

  //
  // Getters
  //
  def getMaxIterations(): Int = $(maxIterations)

  def getStandardize(): Boolean = $(standardize)

  def getInit(): String = $(init)

  def getUserPoints(): Array[Array[Double]] = $(userPoints)

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

  def setUserPoints(value: Array[Array[Double]]): this.type = set(userPoints, value)

  def setEstimateK(value: Boolean): this.type = set(estimateK, value)

  def setK(value: Int): this.type = set(k, value)

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._max_iterations = getMaxIterations()
    parameters._standardize = getStandardize()
    parameters._init = KMeans.Initialization.valueOf(getInit())
    parameters._user_points = {
      val userPoints = getUserPoints()
      if (userPoints == null) {
        null
      } else {
        val spark = SparkSession.builder().getOrCreate()
        import spark.implicits._
        val df = spark.sparkContext.parallelize(userPoints).toDF()
        H2OContext.getOrCreate(spark).asH2OFrame(df).key
      }
    }
    parameters._estimate_k = getEstimateK()
    parameters._k = getK()
  }
}
