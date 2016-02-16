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
      .set("spark.ext.h2o.client.ip", "10.0.0.100")
      .set("spark.ext.h2o.client.port.base", "1267")
      .set("spark.ext.h2o.node.port.base", "32333")
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

    //Just create H2OContext without starting it - we do not want to add public method for H2OContext for creating
    //the context without starting it, so we use reflections to call private constructor
    sc = new SparkContext("local", "test-local", sparkConf)
    val testClass = classOf[H2OContext]
    val ctor = testClass.getDeclaredConstructor(classOf[SparkContext])
    ctor.setAccessible(true)
    hc = ctor.newInstance(sc)

    // Test passed values
    assert(hc.useFlatFile == false)
    assert(hc.numH2OWorkers == 42)
    assert(hc.clientBasePort == 1267)
    assert(hc.nodeBasePort == 32333)
    assert(hc.clientIp == Some("10.0.0.100"))
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
