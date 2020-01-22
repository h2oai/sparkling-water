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

package water.network

import java.nio.file.Files

object SparklingWaterSecurityUtils {
  def generateSSLPair(namePrefix: String): SecurityUtils.SSLCredentials = {
    val nanoTime = System.nanoTime
    val temp = Files.createTempDirectory(s"h2o-internal-jks-$nanoTime")
    temp.toFile.deleteOnExit()
    val name = s"$namePrefix-$nanoTime.jks"
    SecurityUtils.generateSSLPair(SecurityUtils.passwordGenerator(16), name, temp.toAbsolutePath.toString)
  }

  def generateSSLPair(): SecurityUtils.SSLCredentials = generateSSLPair(namePrefix = "h2o-internal")

  def generateSSLConfig(credentials: SecurityUtils.SSLCredentials): String = {
    SecurityUtils.generateSSLConfig(credentials)
  }
}
