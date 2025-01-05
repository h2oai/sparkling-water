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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.algos._
import ai.h2o.sparkling.ml.features.{H2OAutoEncoder, H2OGLRM, H2OPCA, H2OWord2Vec}
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import hex.word2vec.Word2Vec.{NormModel, WordModel}
import org.apache.spark.ml.Estimator
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import scala.reflect.{ClassTag, classTag}

@RunWith(classOf[JUnitRunner])
class MOJOParameterTestSuite extends FunSuite with SharedH2OTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    .withColumn("CAPSULE", 'CAPSULE cast "string")

  test("Test MOJO parameters on GBM") {
    val algorithm = new H2OGBM()
      .setLabelCol("CAPSULE")
      .setSeed(1)
      .setMonotoneConstraints(Map("AGE" -> 1.0, "RACE" -> -1.0))
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on DRF") {
    val algorithm = new H2ODRF()
      .setLabelCol("CAPSULE")
      .setSeed(1)
      .setClassSamplingFactors(Array(0.2f, 1, 1))
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on XGBoost") {
    val algorithm = new H2OXGBoost()
      .setLabelCol("CAPSULE")
      .setSeed(1)
      .setMonotoneConstraints(Map("AGE" -> 1.0, "RACE" -> -1.0))
      .setInteractionConstraints(Array(Array("DPROS", "DCAPS"), Array("PSA", "VOL", "GLEASON")))
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on GLM") {
    val algorithm = new H2OGLM()
      .setLabelCol("CAPSULE")
      .setSeed(1)
      .setFamily("binomial")
      .setAlphaValue(Array(0.5))
      .setLambdaValue(Array(0.5))
      .setMaxIterations(30)
      .setObjectiveEpsilon(0.001)
      .setGradientEpsilon(0.001)
      .setObjReg(0.001)
      .setMaxActivePredictors(3000)
      .setLambdaMinRatio(0.001)
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on GAM") {
    val algorithm = new H2OGAM()
      .setLabelCol("CAPSULE")
      .setSeed(1)
      .setLambdaValue(Array(0.5))
      .setGamCols(Array(Array("PSA"), Array("AGE")))
      .setSplinesNonNegative(Array(true, false))
      .setNumKnots(Array(5, 5))
      .setBs(Array(1, 1))
      .setScale(Array(.5, .5))
      .setSplineOrders(Array(-1, -1))
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo, Set("getFeaturesCols", "getSplinesNonNegative"))
  }

  test("Test MOJO parameters on Deep Learning") {
    val algorithm = new H2ODeepLearning().setLabelCol("CAPSULE").setSeed(1)
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on Autoencoder") {
    val algorithm = new H2OAutoEncoder()
      .setSeed(1)
      .setHidden(Array(3))
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on KMeans") {
    val algorithm = new H2OKMeans().setSeed(1)
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on Isolation Forest") {
    val algorithm = new H2OIsolationForest()
      .setSeed(1)
      .setSampleRate(0.5)
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on RuleFit") {
    val algorithm = new H2ORuleFit()
      .setLabelCol("CAPSULE")
      .setSeed(1)
    val mojo = algorithm.fit(dataset)

    val ignored = Set("getFeaturesCols")
    compareParameterValues(algorithm, mojo, ignored)
  }

  test("Test MOJO parameters on PCA") {
    val algorithm = new H2OPCA()
      .setSeed(1)
      .setInputCols("DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setK(3)
    val mojo = algorithm.fit(dataset)

    val ignoredMethods = Set("getPcaImpl") // PUBDEV-8217: Value of pca_impl isn't propagated to MOJO models

    compareParameterValues(algorithm, mojo, ignoredMethods)
  }

  test("Test MOJO parameters on GLRM") {
    val algorithm = new H2OGLRM()
      .setSeed(1)
      .setInputCols("DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setK(3)
    val mojo = algorithm.fit(dataset)

    compareParameterValues(algorithm, mojo)
  }

  test("Test MOJO parameters on Word2Vec") {
    val algorithm = new H2OWord2Vec()
      .setVecSize(11)
      .setWindowSize(2)
      .setSentSampleRate(0.002f)
      .setNormModel(NormModel.HSM.name())
      .setEpochs(5)
      .setMinWordFreq(1)
      .setInitLearningRate(0.01f)
      .setWordModel(WordModel.CBOW.name())
      .setInputCol("someInputCol")
      .setOutputCol("someOutputCol")

    val mojo = algorithm.fit(Seq(Seq("a", "b", "c"), Seq("c", "b", "a")).toDF("someInputCol"))

    compareParameterValues(algorithm, mojo)
  }

  protected def compareParameterValues[A <: Estimator[H2OMOJOModel]: ClassTag, M <: H2OMOJOModel: ClassTag](
      algorithm: A,
      mojoModel: M,
      ignoredMethods: Set[String] = Set.empty): Unit = {
    val algorithmMethods = classTag[A].runtimeClass.getMethods
    val ignoredMethodsWithGetClass = ignoredMethods + "getClass"
    val algorithmMethodsMap =
      algorithmMethods.map(a => a.getName -> a).filter(a => !ignoredMethodsWithGetClass.contains(a._1)).toMap
    for (mojoMethod <- classTag[M].runtimeClass.getMethods
         if mojoMethod.getName.startsWith("get")
         if mojoMethod.getParameterCount == 0
         if algorithmMethodsMap.contains(mojoMethod.getName)) {
      val mojoValue = mojoMethod.invoke(mojoModel)
      val algorithmValue = algorithmMethodsMap(mojoMethod.getName).invoke(algorithm)
      assert(
        compareValues(mojoValue, algorithmValue),
        s"The value '$mojoValue' from  ${mojoMethod.getName} on " +
          s"the MOJO class is not the same as the value '$algorithmValue' from the same method on the algorithm class.")
    }
  }

  protected def compareValues(mojoValue: AnyRef, algorithmValue: AnyRef): Boolean = (mojoValue, algorithmValue) match {
    case (_, "AUTO") => true
    case (_, "auto") => true
    case (_, "family_default") => true
    case (null, map: Map[_, _]) if map.isEmpty => true
    case (array1: Array[Array[_]], array2: Array[Array[_]]) => array1.map(_.toSeq).toSeq == array2.map(_.toSeq).toSeq
    case (array1: Array[_], array2: Array[_]) => array1.toSeq == array2.toSeq
    case (val1, val2) => val1 == val2
  }
}
