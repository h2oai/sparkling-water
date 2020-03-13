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

package ai.h2o.sparkling.backend.utils

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OConf
import water.network.SecurityUtils.SSLCredentials
import water.network.{SecurityUtils => H2OSecurityUtils}

private[backend] object SecurityUtils {

  def enableSSL(conf: H2OConf): Unit = {
    val spark = SparkSessionUtils.active
    val sslPair = generateSSLPair()
    val config = generateSSLConfig(sslPair)
    conf.set(SharedBackendConf.PROP_SSL_CONF._1, config)
    spark.sparkContext.addFile(sslPair.jks.getLocation)
    if (sslPair.jks.getLocation != sslPair.jts.getLocation) {
      spark.sparkContext.addFile(sslPair.jts.getLocation)
    }
  }

  def enableFlowSSL(conf: H2OConf): H2OConf = {
    val sslPair = generateSSLPair("h2o-internal-auto-flow-ssl")
    conf.setJks(sslPair.jks.getLocation)
    conf.setJksPass(sslPair.jks.pass)
  }

  private def generateSSLPair(): SSLCredentials = generateSSLPair(namePrefix = "h2o-internal")

  private def generateSSLConfig(credentials: SSLCredentials): String = {
    H2OSecurityUtils.generateSSLConfig(credentials)
  }

  private def generateSSLPair(namePrefix: String): SSLCredentials = {
    val nanoTime = System.nanoTime
    val temp = Utils.createTempDir(s"h2o-internal-jks-$nanoTime")
    val name = s"$namePrefix-$nanoTime.jks"
    H2OSecurityUtils.generateSSLPair(H2OSecurityUtils.passwordGenerator(16), name, temp.toPath.toAbsolutePath.toString)
  }
}
