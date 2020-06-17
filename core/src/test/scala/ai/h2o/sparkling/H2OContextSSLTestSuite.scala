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

import java.net.{SocketException, URI}

import ai.h2o.sparkling.backend.api.dataframes.DataFrames
import ai.h2o.sparkling.backend.exceptions.RestApiNotReachableException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.CloudV3
@RunWith(classOf[JUnitRunner])
class H2OContextSSLTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession =
    sparkSession("local[*]", defaultSparkConf.set("spark.ext.h2o.auto.flow.ssl", true.toString))

  test("H2O endpoint behind Flow Proxy is accessible with HTTPS") {
    val cloudV3 = RestApiUtils.query[CloudV3](new URI(s"https://${hc.flowIp}:${hc.flowPort}"), "3/Cloud", hc.getConf)
    assert(cloudV3 != null)
    assert(cloudV3.cloud_name == hc.getConf.cloudName.get)
  }

  test("SW endpoint behind Flow Proxy is accessible with HTTPS") {
    import spark.implicits._
    spark.sparkContext.parallelize(1 to 10).toDF().createOrReplaceTempView("table_name")
    val dataFrames =
      RestApiUtils.query[DataFrames](new URI(s"https://${hc.flowIp}:${hc.flowPort}"), "3/dataframes", hc.getConf)
    assert(dataFrames != null)
    assert(dataFrames.dataframes.head.dataframe_id == "table_name")
  }

  test("H2O endpoint behind Flow Proxy is not accessible without HTTPS") {
    val thrown = intercept[RestApiNotReachableException] {
      RestApiUtils.query[CloudV3](new URI(s"http://${hc.flowIp}:${hc.flowPort}"), "3/Cloud", hc.getConf)
    }
    assert(thrown.getCause.isInstanceOf[SocketException])
    assert(thrown.getCause.getMessage == "Unexpected end of file from server")
  }

  test("SW endpoint behind Flow Proxy is not accessible without HTTPS") {
    val thrown = intercept[RestApiNotReachableException] {
      RestApiUtils.query[DataFrames](new URI(s"http://${hc.flowIp}:${hc.flowPort}"), "3/dataframes", hc.getConf)
    }
    assert(thrown.getCause.isInstanceOf[SocketException])
    assert(thrown.getCause.getMessage == "Unexpected end of file from server")
  }
}
