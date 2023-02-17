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

import ai.h2o.sparkling.H2OFrame.query
import ai.h2o.sparkling.backend.exceptions.RestApiUnauthorisedException
import ai.h2o.sparkling.backend.utils.RestApiUtils
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.PingV3

import java.io.File
import java.net.URI

@RunWith(classOf[JUnitRunner])
class LdapProxyOnlyModeAuthTestSuite extends LdapTestSuiteBase {

  test("SW internally should communicate with h2o-3 in LDAP Proxy only mode without problems") {
    RestApiUtils.getPingInfo(hc.getConf)
  }

  test("H2O endpoint should not be accessible even with correct LDAP credentials") {
    val conf = hc.getConf
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    intercept[RestApiUnauthorisedException](RestApiUtils.getPingInfo(conf))
  }

  test("Flow proxy should be available with correct LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf)
  }

  test("Flow proxy should not be available with wrong LDAP credentials") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword("WRONG_PASSWORD")
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    intercept[RestApiUnauthorisedException](query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf))
  }

  override def createSparkSession(): SparkSession = {
    val tmpFile: File = writeTmpFile("-ldap-login.conf", LdapConnectionConfig)
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.proxy.login.only", "true")
      .set("spark.ext.h2o.ldap.login", "true")
      .set("spark.ext.h2o.user.name", SwClusterOwnerName)
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    val result = sparkSession("local-cluster[2,1,1024]", sparkConf)
    result.sparkContext.addFile(
      classOf[LdapProxyOnlyModeAuthTestSuite].getClassLoader.getResource("log4j.properties").getPath)
    result
  }

}
