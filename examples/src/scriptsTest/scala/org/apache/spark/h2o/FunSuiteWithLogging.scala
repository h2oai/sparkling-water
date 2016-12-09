package org.apache.spark.h2o

import org.apache.spark.Logging
import org.scalatest.FunSuite

/**
  * Friend trait to expose private Spark logging to our test infrastructure
  * which is located in different package.
  */
trait FunSuiteWithLogging extends FunSuite with Logging {

}
