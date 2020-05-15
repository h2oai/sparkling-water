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
package ai.h2o.sparkling

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Test passing parameters via SparkConf.
  */
@RunWith(classOf[JUnitRunner])
class H2OConfTestSuite extends FunSuite with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]", createConf())

  test("test H2OConf parameters") {
    val conf = new H2OConf().setClusterSize(1)

    assert(conf.numH2OWorkers.contains(42))
    assert(conf.basePort == 32333)
    assert(conf.clientIp.contains("10.0.0.100"))
    assert(conf.cloudTimeout == 10 * 1000)
    assert(conf.numRddRetries == 2)
    assert(conf.cloudName.isDefined)
    assert(conf.cloudName.contains("test-sparkling-cloud-"))
    assert(conf.logLevel == "DEBUG")
    assert(conf.clientNetworkMask.contains("127.0.0.1/32"))
    assert(conf.nodeNetworkMask.contains("0.0.0.1/24"))
    assert(conf.nthreads == 7)
    assert(conf.clientWebPort == 13321)
    assert(conf.drddMulFactor == 2)
  }

  private def createConf() = {
    new SparkConf()
      .set("spark.ext.h2o.cluster.size", "42")
      .set("spark.ext.h2o.client.ip", "10.0.0.100")
      .set("spark.ext.h2o.base.port", "32333")
      .set("spark.ext.h2o.cloud.timeout", (10 * 1000).toString)
      .set("spark.ext.h2o.spreadrdd.retries", "2")
      .set("spark.ext.h2o.cloud.name", "test-sparkling-cloud-")
      .set("spark.ext.h2o.log.level", "DEBUG")
      .set("spark.ext.h2o.client.network.mask", "127.0.0.1/32")
      .set("spark.ext.h2o.node.network.mask", "0.0.0.1/24")
      .set("spark.ext.h2o.nthreads", "7")
      .set("spark.ext.h2o.client.web.port", "13321")
      .set("spark.ext.h2o.dummy.rdd.mul.factor", "2")
  }
}
