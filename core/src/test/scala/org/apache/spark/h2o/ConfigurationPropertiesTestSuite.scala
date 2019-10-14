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

import java.net.{HttpURLConnection, URL}
import java.security.Permission

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SparkTestContext
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite extends FunSuite with Matchers with SparkTestContext {

  @transient var hc: H2OContext = null

  test("test extra HTTP headers are propagated to FLOW UI") {
    sc = new SparkContext("local[*]", this.getClass.getSimpleName, defaultSparkConf)
    val h2oConf = new H2OConf(spark)
    val extraHttpHeaders = Map(
      "X-MyCustomHeaderA" -> "A",
      "X-MyCustomHeaderB" -> "B")
    h2oConf
      .setFlowExtraHttpHeaders(extraHttpHeaders)
      .setClusterSize(1)
      .set("spark.ext.h2o.backend.cluster.mode", "external")
    hc = H2OContext.getOrCreate(spark, h2oConf)

    val url = new URL(hc.flowURL())
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      val flowHeaders = connection.getHeaderFields.asScala.filterKeys(key => extraHttpHeaders.contains(key)).toMap
      flowHeaders shouldEqual extraHttpHeaders.mapValues(List(_).asJava)
    }
    finally {
      connection.disconnect()
    }
  }

  override def afterAll() {
    // The method H2O.exit calls System.exit which confuses Gradle and marks the build
    // as successful even though some tests failed.
    // We can solve this by using security manager which forbids System.exit call.
    // It is safe to use as all the methods closing H2O cloud and stopping operations have been
    // already called and we just need to ensure that JVM with the client/driver doesn't call the System.exit method
    try {
      val securityManager = new NoExitCheckSecurityManager
      System.setSecurityManager(securityManager)
      if (hc != null){
        hc.stop()
        hc = null
      }
      resetSparkContext()
      super.afterAll()
      System.setSecurityManager(null)
    } catch {
      case _: SecurityException => // ignore
    }
  }

  private class NoExitCheckSecurityManager extends SecurityManager {
    override def checkPermission(perm: Permission): Unit = {
      /* allow any */
    }

    override def checkPermission(perm: Permission, context: scala.Any): Unit = {
      /* allow any */
    }

    override def checkExit(status: Int): Unit = {
      super.checkExit(status)
      throw new SecurityException()
    }
  }
}
