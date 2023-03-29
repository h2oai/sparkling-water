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
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.api.schemas3.PingV3

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Base64

@RunWith(classOf[JUnitRunner])
class LdapBase64LoginModuleSuite extends LdapTestSuiteBase {

  private val LdapAdminPasswordB64 =
    Base64.getEncoder.encodeToString(LdapAdminPassword.getBytes(StandardCharsets.UTF_8))
  private val LdapBase64ConnectionConfig =
    s"""
       |ldaploginmodule {
       |    water.webserver.jetty9.LdapBase64LoginModule required
       |    debug="true"
       |    useLdaps="false"
       |    contextFactory="com.sun.jndi.ldap.LdapCtxFactory"
       |    hostname="localhost"
       |    port="$LdapPort"
       |    bindDn="cn=$LdapAdmin,$LdapBaseDn"
       |    base64BindPassword="$LdapAdminPasswordB64"
       |    authenticationMethod="simple"
       |    forceBindingLogin="true"
       |    userBaseDn="$LdapBaseDn";
       |};
       |""".stripMargin

  test("LdapBase64LoginModule should allow the binding") {
    val conf = new H2OConf()
      .setUserName(SwClusterOwnerName)
      .setPassword(SwClusterOwnerPassword)
      .setProxyLoginOnlyDisabled() //avoiding internal credentials generation in h2oconf, query method used to do a check
    query[PingV3](new URI(hc.flowURL()), "/3/Ping", conf)
  }

  override def createSparkSession(): SparkSession = {
    val tmpFile: File = writeTmpFile("-ldap-login.conf", LdapBase64ConnectionConfig)
    val sparkConf = defaultSparkConf
      .set("spark.ext.h2o.ldap.login", "true")
      .set("spark.ext.h2o.user.name", SwClusterOwnerName)
      .set("spark.ext.h2o.proxy.login.only", "true")
      .set("spark.ext.h2o.login.conf", tmpFile.getAbsolutePath)
    val result = sparkSession("local-cluster[2,1,1024]", sparkConf)
    result.sparkContext.addFile(
      classOf[LdapBase64LoginModuleSuite].getClassLoader.getResource("log4j.properties").getPath)
    result
  }

}
