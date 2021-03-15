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

import java.io.File
import java.net.{SocketException, URI}

import ai.h2o.sparkling.backend.api.dataframes.DataFrames
import ai.h2o.sparkling.backend.exceptions.RestApiNotReachableException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.CloudV3

trait TestingCertificates {
  protected def locateCertificate(fileName: String): String = {
    val basePath = "./core/src/integTest/certificates/"
    val file = new File(basePath + fileName)
    if (file.exists()) {
      file.getAbsolutePath
    } else {
      // testing from IDEA
      new File("." + basePath + fileName).getAbsolutePath
    }
  }
}

abstract class H2OContextFlowSSLTestSuiteBase extends FunSuite with SharedH2OTestContext with TestingCertificates {

  def sparkConf: SparkConf

  override def createSparkSession(): SparkSession =
    sparkSession("local-cluster[3, 1, 1024]", sparkConf)

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

  test("Convert DataFrame to H2OFrame with Flow SSL enabled") {
    import spark.implicits._
    val dataset = spark.range(1000).select('id, 'id.cast(StringType).as("id_string"))

    val frame = hc.asH2OFrame(dataset)
    val result = hc.asSparkFrame(frame)

    TestUtils.assertDataFramesAreIdentical(dataset, result)
  }
}

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_AutoSSLEnabled extends H2OContextFlowSSLTestSuiteBase {
  override def sparkConf: SparkConf = defaultSparkConf.set("spark.ext.h2o.auto.flow.ssl", true.toString)
}

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_AutoSSLAndSecureInternalConnectionsEnabled extends H2OContextFlowSSLTestSuiteBase {
  override def sparkConf: SparkConf =
    defaultSparkConf
      .set("spark.ext.h2o.auto.flow.ssl", true.toString)
      .set("spark.ext.h2o.internal_secure_connections", false.toString)
}

abstract class H2OContextFlowSSLTestSuite_CertificateVerificationDisabledBase(certificateName: String)
  extends H2OContextFlowSSLTestSuiteBase {

  val certificatePath = locateCertificate(certificateName)
  override def sparkConf: SparkConf =
    defaultSparkConf
      .set("spark.ext.h2o.jks", certificatePath)
      .set("spark.ext.h2o.jks.pass", "h2oh2o")
      .set("spark.ext.h2o.jks.alias", "h2o")
      .set("spark.ext.h2o.verify_ssl_certificates", false.toString)
}

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_CertificateVerificationDisable_SelfSigned
  extends H2OContextFlowSSLTestSuite_CertificateVerificationDisabledBase("selfSigned.jks")

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_CertificateVerificationDisable_SignedByFake
  extends H2OContextFlowSSLTestSuite_CertificateVerificationDisabledBase("signedByFake.jks")

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_CACertificateInTrustStore_SignedByFake extends H2OContextFlowSSLTestSuiteBase {

  val certificatePath = locateCertificate("signedByFake.jks")
  val caCertificatePath = locateCertificate("fake.jks")

  System.setProperty("javax.net.ssl.trustStore", caCertificatePath)

  override def sparkConf: SparkConf =
    defaultSparkConf
      .set("spark.ext.h2o.jks", certificatePath)
      .set("spark.ext.h2o.jks.pass", "h2oh2o")
      .set("spark.ext.h2o.jks.alias", "h2o")
      .set("spark.ext.h2o.verify_ssl_hostnames", false.toString)
      .set("spark.executor.extraJavaOptions", s"-Djavax.net.ssl.trustStore=$caCertificatePath")
}

abstract class H2OContextFlowSSLTestSuite_CertificateVerificationEnabledBase(certificateName: String)
  extends FunSuite
  with SparkTestContext
  with TestingCertificates {
  override def createSparkSession(): SparkSession = sparkSession("local-cluster[2, 1, 1024]")

  test(s"H2OContext won't start since the $certificateName won't be verified") {
    val certificatePath = locateCertificate(certificateName)
    val conf = new H2OConf()
      .set("spark.ext.h2o.jks", certificatePath)
      .set("spark.ext.h2o.jks.pass", "h2oh2o")
      .set("spark.ext.h2o.jks.alias", "h2o")
      .set("spark.ext.h2o.verify_ssl_hostnames", false.toString)

    intercept[NoSuchElementException] { // No reference to H2O cluster.
      H2OContext.getOrCreate(conf)
    }
  }
}

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_CertificateVerificationEnabled_SelfSigned
  extends H2OContextFlowSSLTestSuite_CertificateVerificationEnabledBase("selfSigned.jks")

@RunWith(classOf[JUnitRunner])
class H2OContextFlowSSLTestSuite_CertificateVerificationEnabled_SignedByFake
  extends H2OContextFlowSSLTestSuite_CertificateVerificationEnabledBase("signedByFake.jks")
