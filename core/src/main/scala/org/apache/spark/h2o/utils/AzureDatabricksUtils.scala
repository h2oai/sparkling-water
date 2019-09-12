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

package org.apache.spark.h2o.utils

import org.apache.spark.SparkContext

private[h2o] object AzureDatabricksUtils {
  val externalFlowPort = 9009 //This port is exposed in Azure DBC

  def getDBCAzureFlowURL(sc: SparkContext): String = {
    val clusterId = sc.getConf.get("spark.databricks.clusterUsageTags.clusterId")
    val orgId = sc.getConf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

    val dbcConfFile = scala.io.Source.fromFile("/databricks/common/conf/deploy.conf")
    val line = dbcConfFile.getLines.find(_.contains("databricks.region.name")).get.trim()
    val region = line.split("=")(1).trim().replaceAll("\"", "")

    val azureHost = s"https://${region}.azuredatabricks.net"
    s"$azureHost/driver-proxy/o/$orgId/$clusterId/$externalFlowPort/flow/index.html"
  }

}
