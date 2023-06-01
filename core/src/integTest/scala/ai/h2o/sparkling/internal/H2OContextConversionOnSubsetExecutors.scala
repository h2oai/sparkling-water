package ai.h2o.sparkling.internal

import ai.h2o.sparkling.TestUtils.{DoubleHolder, assertRDDHolderProperties, assertVectorIntValues}
import ai.h2o.sparkling.backend.internal.InternalBackendConf
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext, TestUtils}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * This test is being run only in internal backend mode as it does not make sense
  * to verify this functionality on external backend where we run H2O outside of Spark
  */
@RunWith(classOf[JUnitRunner])
class H2OContextConversionOnSubsetExecutors extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession =
    sparkSession("local-cluster[3,1,1024]", defaultSparkConf.set(InternalBackendConf.PROP_CLUSTER_SIZE._1, "1"))

  test("asH2OFrame conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    val data = 1 to 1000
    val rdd = sc.parallelize(data, 100).map(v => Some(v))
    val h2oFrame = hc.asH2OFrame(rdd)
    assertRDDHolderProperties(h2oFrame, rdd)
    assertVectorIntValues(h2oFrame.collectInts(0), data)
    h2oFrame.delete()
    rdd.unpersist()
  }

  test("asSparkFrame conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    val originalRdd = sc.parallelize(1 to 1000, 100).map(v => Some(v))
    val hf = hc.asH2OFrame(originalRdd)

    val convertedDf = hc.asSparkFrame(hf)
    import spark.implicits._
    TestUtils.assertDataFramesAreIdentical(originalRdd.toDF, convertedDf.toDF())
  }

  test("asRDD conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    import spark.implicits._
    val originalRdd = sc.parallelize(1 to 1000, 100).map(v => Some(v))
    val hf = hc.asH2OFrame(originalRdd)

    val convertedRdd = hc.asRDD[DoubleHolder](hf)

    TestUtils.assertDataFramesAreIdentical(originalRdd.toDF, convertedRdd.toDF())
  }
}
