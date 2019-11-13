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
import java.nio.file.{Files, Path}
import java.security.Permission

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

import collection.JavaConverters._


abstract class ConfigurationPropertiesTestSuite extends FunSuite with Matchers with SparkTestContext {

  @transient var hc: H2OContext = _

  def createSparkSession(master: String): SparkSession = {
    System.setProperty("spark.test.home", System.getenv("SPARK_HOME"))
    val conf = defaultSparkConf
      .set("spark.driver.extraClassPath", sys.props("java.class.path"))
      .set("spark.executor.extraClassPath", sys.props("java.class.path"))
    sc = new SparkContext(master, getClass.getSimpleName, conf)
    SparkSession.builder().sparkContext(sc).getOrCreate()
  }

  override def afterEach(): Unit = {
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
      }
    } catch {
      case _: SecurityException => // ignore
    } finally {
      hc = null
      resetSparkContext()
      super.afterAll()
      System.setSecurityManager(null)
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

abstract class ConfigurationPropertiesTestSuite_HttpHeadersBase extends ConfigurationPropertiesTestSuite {

  def testExtraHTTPHeadersArePropagated(master: String, urlProvider: H2OContext => String): Unit = {
    val spark = createSparkSession(master)
    val h2oConf = new H2OConf(spark)
    val extraHttpHeaders = Map(
      "X-MyCustomHeaderA" -> "A",
      "X-MyCustomHeaderB" -> "B")
    h2oConf
      .setFlowExtraHttpHeaders(extraHttpHeaders)
      .setH2ONodeWebEnabled()
      .setClusterSize(1)
      .useAutoClusterStart()
    hc = H2OContext.getOrCreate(spark, h2oConf)

    val url = new URL(urlProvider(hc))
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      val flowHeaders = connection.getHeaderFields.asScala.filterKeys(key => extraHttpHeaders.contains(key)).toMap
      flowHeaders shouldEqual extraHttpHeaders.mapValues(List(_).asJava)
    }
    finally {
      connection.disconnect()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_HttpHeadersOnClient extends ConfigurationPropertiesTestSuite_HttpHeadersBase {
  test("test extra HTTP headers are propagated to FLOW UI") {
    testExtraHTTPHeadersArePropagated("local[*]", (hc: H2OContext) => hc.flowURL())
  }
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_HttpHeadersOnNode extends ConfigurationPropertiesTestSuite_HttpHeadersBase {
  test("test extra HTTP headers are propagated to FLOW UI") {
    testExtraHTTPHeadersArePropagated("local-cluster[1,1,1024]", (hc: H2OContext) => s"http://${hc.h2oNodes.head.ipPort}")
  }
}

abstract class ConfigurationPropertiesTestSuite_NotifyLocalBase extends ConfigurationPropertiesTestSuite {

  def testNotifyLocalPropertyCreatesFile(master: String, propertySetter: (H2OConf, Path) => H2OConf): Unit = {
    val spark = createSparkSession(master)
    val tmpDirPath = Files.createTempDirectory(s"SparklingWater-${getClass.getSimpleName}").toAbsolutePath
    val tmpDir = tmpDirPath.toFile
    tmpDir.setWritable(true, false)
    tmpDir.setReadable(true, false)
    tmpDir.setExecutable(true, false)
    tmpDir.deleteOnExit()
    val filePath = tmpDirPath.resolve("file")
    val file = filePath.toFile
    file.deleteOnExit()

    val h2oConf = propertySetter(new H2OConf(spark).setClusterSize(1), filePath)
    hc = H2OContext.getOrCreate(spark, h2oConf)

    assert(file.exists(), s"H2O process didn't create a file on the path '$filePath'.")
  }

  def setExtraClientProperties(conf: H2OConf, filePath: Path): H2OConf = conf.setClientExtraProperties(s"-notify_local $filePath")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaClientExtraProperties_Local extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  test("test that notify_local set via client extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile("local[*]", setExtraClientProperties)
  }
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaClientExtraProperties_LocalCluster extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  test("test that notify_local set via client extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile("local-cluster[1,1,1024]", setExtraClientProperties)
  }
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaNodeExtraProperties extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  def setExtraNodeProperties(conf: H2OConf, filePath: Path): H2OConf = {
    val properties = if (conf.backendClusterMode == "external") {
      s"-J -notify_local -J $filePath"
    } else {
      s"-notify_local $filePath"
    }
    conf.setNodeExtraProperties(properties)
  }

  test("test that notify_local set via node extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile("local-cluster[1,1,1024]", setExtraNodeProperties)
  }
}
