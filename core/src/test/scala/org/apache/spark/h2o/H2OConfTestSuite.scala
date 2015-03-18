package org.apache.spark.h2o

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.h2o.util.SparkTestContext
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

/**
 * Test passing parameters via SparkConf.
 */
@RunWith(classOf[JUnitRunner])
class H2OConfTestSuite extends FunSuite
with Matchers with BeforeAndAfter with SparkTestContext {

  test("test H2OConf parameters") {
    val sparkConf = new SparkConf()
      .set("spark.ext.h2o.flatfile", "false")
      .set("spark.ext.h2o.cluster.size", "42")
      .set("spark.ext.h2o.port.base", "32333")
      .set("spark.ext.h2o.cloud.timeout", (10*1000).toString)
      .set("spark.ext.h2o.spreadrdd.retries", "2")
      .set("spark.ext.h2o.cloud.name", "test-sparkling-cloud-")
      .set("spark.ext.h2o.node.log.level", "DEBUG")
      .set("spark.ext.h2o.client.log.level", "DEBUG")
      .set("spark.ext.h2o.client.log.level", "DEBUG")
      .set("spark.ext.h2o.network.mask", "127.0.0.1/32")
      .set("spark.ext.h2o.nthreads", "7")
      .set("spark.ext.h2o.disable.ga", "true")
      .set("spark.ext.h2o.client.web.port", "13321")
      .set("spark.ext.h2o.dummy.rdd.mul.factor", "2")
    sc = new SparkContext("local", "test-local", sparkConf)
    hc = new H2OContext(sc)

    // Test passed values
    assert(hc.useFlatFile == false)
    assert(hc.numH2OWorkers == 42)
    assert(hc.basePort == 32333)
    assert(hc.cloudTimeout == 10*1000)
    assert(hc.numRddRetries == 2)
    assert(hc.cloudName == "test-sparkling-cloud-")
    assert(hc.h2oNodeLogLevel == "DEBUG")
    assert(hc.h2oClientLogLevel == "DEBUG")
    assert(hc.networkMask == Some("127.0.0.1/32"))
    assert(hc.nthreads == 7)
    assert(hc.disableGA == true)
    assert(hc.clientWebPort == 13321)
    assert(hc.drddMulFactor == 2)

    resetContext()
  }

  class DummyH2OContext(val sparkConf: SparkConf) extends H2OConf
}