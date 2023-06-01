package ai.h2o.sparkling.backend.utils

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.SharedBackendConf
import org.apache.spark.SparkConf
import org.apache.spark.expose.Logging

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

  def isRunningOnAzureDatabricks(conf: H2OConf): Boolean = isRunningOnAzureDatabricks(conf.sparkConf)

  def isRunningOnAzureDatabricks(conf: SparkConf): Boolean = {
    conf.getOption("spark.databricks.cloudProvider").contains("Azure")
  }

  def relativeFlowURL(conf: H2OConf): String = {
    val clusterId = conf.get("spark.databricks.clusterUsageTags.clusterId")
    val orgId = conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
    s"/driver-proxy/o/$orgId/$clusterId/${conf.clientWebPort}/flow/index.html"
  }
}
