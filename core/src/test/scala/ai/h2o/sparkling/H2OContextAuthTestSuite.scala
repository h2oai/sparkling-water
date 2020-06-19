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

import java.io.{IOException, PrintWriter}
import java.net.URI

import ai.h2o.sparkling.backend.api.dataframes.DataFrames
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.CloudV3

import scala.io.Source

@RunWith(classOf[JUnitRunner])
class H2OContextAuthTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession =
    sparkSession(
      "local[*]",
      defaultSparkConf
        .set("spark.ext.h2o.login.conf", createLoginConf())
        .set("spark.ext.h2o.hash.login", "true")
        .set("spark.ext.h2o.user.name", "admin")
        .set("spark.ext.h2o.password", "admin_password"))

  private def getFlowIp(): String = {
    val field = hc.getClass.getDeclaredField("flowIp")
    field.setAccessible(true)
    field.get(hc).asInstanceOf[String]
  }

  private def getFlowPort(): Int = {
    val field = hc.getClass.getDeclaredField("flowPort")
    field.setAccessible(true)
    field.get(hc).asInstanceOf[Int]
  }

  private def createLoginConf(): String = {
    val filename = "build/login.conf"
    new PrintWriter(filename) { write("admin:admin_password"); close() }
    filename
  }

  test("H2O endpoint behind Flow Proxy is accessible with provided username and password") {
    val cloudV3 =
      RestApiUtils.query[CloudV3](new URI(s"http://${getFlowIp()}:${getFlowPort()}"), "3/Cloud", hc.getConf)
    assert(cloudV3 != null)
    assert(cloudV3.cloud_name == hc.getConf.cloudName.get)
  }

  test("SW endpoint behind Flow Proxy is accessible with provided username and password") {
    import spark.implicits._
    spark.sparkContext.parallelize(1 to 10).toDF().createOrReplaceTempView("table_name")
    val dataFrames =
      RestApiUtils.query[DataFrames](new URI(s"http://${getFlowIp()}:${getFlowPort()}"), "3/dataframes", hc.getConf)
    assert(dataFrames != null)
    assert(dataFrames.dataframes.head.dataframe_id == "table_name")
  }

  test("H2O endpoint behind Flow Proxy is not accessible without provided username and password") {
    val url = s"http://${getFlowIp()}:${getFlowPort()}/3/Cloud"
    val thrown = intercept[IOException] {
      Source.fromURL(url)
    }
    assert(thrown.getMessage == s"Server returned HTTP response code: 401 for URL: $url")
  }

  test("SW endpoint behind Flow Proxy is not accessible without provided username and password") {
    val url = s"http://${getFlowIp()}:${getFlowPort()}/3/dataframes"
    val thrown = intercept[IOException] {
      Source.fromURL(url)
    }
    assert(thrown.getMessage == s"Server returned HTTP response code: 401 for URL: $url")
  }
}
