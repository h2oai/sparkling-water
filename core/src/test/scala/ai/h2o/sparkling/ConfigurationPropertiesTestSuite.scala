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

import java.net.{HttpURLConnection, URL}
import java.nio.file.{Files, Path}

import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

import scala.collection.JavaConverters._

abstract class ConfigurationPropertiesTestSuite_HttpHeadersBase extends FunSuite with BeforeAndAfterEach with Matchers with SharedH2OTestContext {

  def testExtraHTTPHeadersArePropagated(urlProvider: H2OContext => String): Unit = {
    val h2oConf = new H2OConf()
    val extraHttpHeaders = Map("X-MyCustomHeaderA" -> "A", "X-MyCustomHeaderB" -> "B")
    h2oConf
      .setFlowExtraHttpHeaders(extraHttpHeaders)
      .setClusterSize(1)
    hc = H2OContext.getOrCreate(h2oConf)

    val url = new URL(urlProvider(hc))
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    try {
      val flowHeaders = connection.getHeaderFields.asScala.filterKeys(key => extraHttpHeaders.contains(key)).toMap
      flowHeaders shouldEqual extraHttpHeaders.mapValues(List(_).asJava)
    } finally {
      connection.disconnect()
    }
  }
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_HttpHeadersOnClient extends ConfigurationPropertiesTestSuite_HttpHeadersBase {
  test("test extra HTTP headers are propagated to FLOW UI") {
    testExtraHTTPHeadersArePropagated((hc: H2OContext) => hc.flowURL())
  }

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_HttpHeadersOnNode extends ConfigurationPropertiesTestSuite_HttpHeadersBase {
  test("test extra HTTP headers are propagated to FLOW UI") {
    testExtraHTTPHeadersArePropagated((hc: H2OContext) => s"http://${hc.getH2ONodes().head.ipPort()}")
  }

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[1,1,1024]")
}

abstract class ConfigurationPropertiesTestSuite_NotifyLocalBase extends FunSuite with BeforeAndAfterEach with Matchers with SharedH2OTestContext {

  def testNotifyLocalPropertyCreatesFile(propertySetter: (H2OConf, Path) => H2OConf): Unit = {
    val tmpDirPath = Files.createTempDirectory(s"SparklingWater-${getClass.getSimpleName}").toAbsolutePath
    val tmpDir = tmpDirPath.toFile
    tmpDir.setWritable(true, false)
    tmpDir.setReadable(true, false)
    tmpDir.setExecutable(true, false)
    tmpDir.deleteOnExit()
    val filePath = tmpDirPath.resolve("file")
    val file = filePath.toFile
    file.deleteOnExit()

    val h2oConf = propertySetter(new H2OConf().setClusterSize(1), filePath)
    hc = H2OContext.getOrCreate(h2oConf)

    Thread.sleep(5000)

    assert(file.exists(), s"H2O process didn't create a file on the path '$filePath'.")
  }

  def setExtraClientProperties(conf: H2OConf, filePath: Path): H2OConf =
    conf.setClientExtraProperties(s"-notify_local $filePath")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaClientExtraProperties_Local
  extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  test("test that notify_local set via client extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile(setExtraClientProperties)
  }

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaClientExtraProperties_LocalCluster
  extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  test("test that notify_local set via client extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile(setExtraClientProperties)
  }

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[1,1,1024]")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_SetNotifyLocalViaNodeExtraProperties
  extends ConfigurationPropertiesTestSuite_NotifyLocalBase {

  def setExtraNodeProperties(conf: H2OConf, filePath: Path): H2OConf = {
    val properties = if (conf.backendClusterMode == "external") {
      s"-J -notify_local -J $filePath"
    } else {
      s"-notify_local $filePath"
    }
    conf.setNodeExtraProperties(properties)
  }

  test("test that notify_local set via node extra properties produce a file") {
    testNotifyLocalPropertyCreatesFile(setExtraNodeProperties)
  }

  override def createSparkSession(): SparkSession = sparkSession("local-cluster[1,1,1024]")
}

abstract class ConfigurationPropertiesTestSuite_ExternalCommunicationCompression(compressionType: String)
  extends FunSuite with BeforeAndAfterEach with Matchers with SharedH2OTestContext {

    test(s"Convert dataset from Spark to H2O and back with $compressionType compression") {
      val h2oConf = new H2OConf()
      h2oConf.setExternalCommunicationCompression(compressionType)
      h2oConf.setClusterSize(1)
      hc = H2OContext.getOrCreate(h2oConf)
      val dataset = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

      val frameKey = hc.asH2OFrameKeyString(dataset)
      val result = hc.asDataFrame(frameKey)

      TestUtils.assertDataFramesAreIdentical(dataset, result)
    }

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
}

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_ExternalCommunicationCompression_NONE
  extends ConfigurationPropertiesTestSuite_ExternalCommunicationCompression("NONE")

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_ExternalCommunicationCompression_SNAPPY
  extends ConfigurationPropertiesTestSuite_ExternalCommunicationCompression("SNAPPY")

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_ExternalCommunicationCompression_DEFLATE
  extends ConfigurationPropertiesTestSuite_ExternalCommunicationCompression("DEFLATE")

@RunWith(classOf[JUnitRunner])
class ConfigurationPropertiesTestSuite_ExternalCommunicationCompression_GZIP
  extends ConfigurationPropertiesTestSuite_ExternalCommunicationCompression("GZIP")
