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

import java.io.FileNotFoundException

import ai.h2o.sparkling.backend.SharedBackendConf
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf

object AzureDatabricksUtils extends Logging {
  private val externalFlowPort = 9009 // This port is exposed in Azure DBC
  private val defaultIncreasedTimeout = 600000

  def setClientWebPort(conf: H2OConf): Int = {
    if (conf.clientWebPort == SharedBackendConf.PROP_CLIENT_WEB_PORT._2) {
      logInfo("Overriding client web port to " + externalFlowPort)
      conf.setClientWebPort(externalFlowPort)
    }
    conf.clientWebPort
  }

  def setClientCheckRetryTimeout(conf: H2OConf): Int = {
    if (conf.clientCheckRetryTimeout == SharedBackendConf.PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2) {
      logInfo("Overriding client check retry timeout to " + defaultIncreasedTimeout)
      conf.setClientCheckRetryTimeout(defaultIncreasedTimeout)
    }
    conf.clientCheckRetryTimeout
  }

  def isRunningOnAzureDatabricks(conf: H2OConf): Boolean = {
    conf.getOption("spark.databricks.cloudProvider").contains("Azure")
  }

  def flowURL(conf: H2OConf): String = {
    val clusterId = conf.get("spark.databricks.clusterUsageTags.clusterId")
    val orgId = conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    val azureHost = s"https://${azureRegion()}.azuredatabricks.net"
    s"$azureHost/driver-proxy/o/$orgId/$clusterId/${conf.clientWebPort}/flow/index.html"
  }

  private def azureRegion(): String = {
    val substRegion = "YOUR_AZURE_REGION"
    val region = try {
      val confFile = scala.io.Source.fromFile("/databricks/common/conf/deploy.conf")
      confFile.getLines
        .find(_.contains("databricks.region.name"))
        .map(_.trim.split("=")(1).trim().replaceAll("\"", ""))
        .getOrElse(substRegion)
    } catch {
      case e: FileNotFoundException => substRegion
    }

    if (region == substRegion) {
      logWarning("Azure region could not be determined automatically, please replace " +
        s"'$substRegion' in the provided flow URL with your region.")
    }
    region
  }
}
