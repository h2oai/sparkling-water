package org.apache.spark.ml.spark.models

import ai.h2o.sparkling.ml.algos.{H2OGBM, H2OGLM}
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OSupervisedMOJOModelTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", getClass.getSimpleName, conf = defaultSparkConf)

  lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))


  test("Offset column gets propagated to MOJO model") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setOffsetCol("PSA")

    val model = algo.fit(dataset)
    val modelOffset = model.getOffsetCol()

    modelOffset shouldEqual algo.getOffsetCol()
  }

  test("Offset column gets propagated when MOJO model serialized and deserialized") {
    val algo = new H2OGLM()
      .setSplitRatio(0.8)
      .setSeed(1)
      .setFeaturesCols("CAPSULE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON")
      .setLabelCol("AGE")
      .setOffsetCol("PSA")

    val model = algo.fit(dataset)
    val modelOffset = model.getOffsetCol()

    modelOffset shouldEqual algo.getOffsetCol()
  }
}
