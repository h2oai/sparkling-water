package org.apache.spark

import org.apache.spark.h2o.H2OContext
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}

/**
 * Testing creation of H2O cloud in distributed environment.
 */
class DistributedSuite extends FunSuite
  with Matchers with BeforeAndAfter with LocalSparkContext {

  ignore("verify H2O cloud building") {
    sc = new SparkContext("local-cluster[3,2,768]", "test")
    hc = new H2OContext(sc).start()
    resetSparkContext()
  }
}
