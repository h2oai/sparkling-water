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

import org.apache.spark.h2o.utils.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

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
      .set("spark.ext.h2o.client.network.mask", "127.0.0.1/32")
      .set("spark.ext.h2o.node.network.mask", "0.0.0.1/24")
      .set("spark.ext.h2o.nthreads", "7")
      .set("spark.ext.h2o.client.web.port", "13321")
      .set("spark.ext.h2o.dummy.rdd.mul.factor", "2")

    val spark = SparkSession.builder().master("local").appName("test-local").config(sparkConf).getOrCreate()

    // We don't need to have H2OContext here started and since it has private constructor
    // and getOrCreate methods automatically start H2OContext, we use a little bit of reflection
    val ctor = classOf[H2OContext].getDeclaredConstructor(classOf[SparkSession], classOf[H2OConf])
    ctor.setAccessible(true)
    val hc = ctor.newInstance(spark, new H2OConf(spark).setNumOfExternalH2ONodes(1))
    val conf = hc.getConf

    // Test passed values
    assert(!conf.useFlatFile)
    assert(conf.numH2OWorkers == Some(42))
    assert(conf.clientBasePort == 1267)
    assert(conf.nodeBasePort == 32333)
    assert(conf.clientIp == Some("10.0.0.100"))
    assert(conf.cloudTimeout == 10*1000)
    assert(conf.numRddRetries == 2)
    assert(conf.cloudName.isDefined)
    assert(conf.cloudName == Some("test-sparkling-cloud-"))
    assert(conf.h2oNodeLogLevel == "DEBUG")
    assert(conf.h2oClientLogLevel == "DEBUG")
    assert(conf.clientNetworkMask == Some("127.0.0.1/32"))
    assert(conf.nodeNetworkMask == Some("0.0.0.1/24"))
    assert(conf.nthreads == 7)
    assert(conf.clientWebPort == 13321)
    assert(conf.drddMulFactor == 2)

    resetSparkContext()
  }
}
