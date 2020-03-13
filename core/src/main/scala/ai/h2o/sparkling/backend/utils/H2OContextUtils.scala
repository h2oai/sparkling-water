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

import java.text.SimpleDateFormat
import java.util.Date

import ai.h2o.sparkling.backend.exceptions.{H2OClusterNotReachableException, RestApiException}
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{BuildInfo, H2OConf}
import water.api.schemas3.CloudLockV3
import water.fvec.Frame

private[utils] trait H2OContextUtils extends RestCommunication with RestApiUtils {

  protected def withConversionDebugPrints[R <: Frame](sc: SparkContext, conversionName: String, block: => R): R = {
    val propName = "spark.h2o.measurements.timing"
    val performancePrintConf = sc.getConf.getOption(propName).orElse(sys.props.get(propName))

    if (performancePrintConf.nonEmpty && performancePrintConf.get.toBoolean) {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      logInfo(s"Elapsed time of the $conversionName conversion into H2OFrame ${result._key}: " + (t1 - t0) / 1000 + " millis")
      result
    } else {
      block
    }
  }

  /**
   * Open browser for given address.
   *
   * @param uri address to open in browser, e.g., http://example.com
   */
  protected def openURI(uri: String): Unit = {
    import java.awt.Desktop
    if (Desktop.isDesktopSupported) {
      Desktop.getDesktop.browse(new java.net.URI(uri))
    } else {
      logWarning(s"Desktop support is missing! Cannot open browser for $uri")
    }
  }

  protected def logFileName(): String = {
    val pattern = "yyyyMMdd_hhmmss"
    val formatter = new SimpleDateFormat(pattern)
    val now = formatter.format(new Date)
    s"h2ologs_$now"
  }

  protected def verifyLogContainer(logContainer: String): Unit = {
    if (!Seq("ZIP", "LOG").contains(logContainer)) {
      throw new IllegalArgumentException(s"Supported LOG container is either LOG or ZIP, specified was: $logContainer")
    }
  }

  protected def getAndVerifyWorkerNodes(conf: H2OConf): Array[NodeDesc] = {
    try {
      lockCloud(conf)
      val nodes = RestApiUtils.getNodes(conf)
      verifyWebOpen(nodes, conf)
      if (!conf.isBackendVersionCheckDisabled) {
        verifyVersion(conf, nodes)
      }
      val leaderIpPort = RestApiUtils.getLeaderNode(conf).ipPort()
      if (conf.h2oCluster.get != leaderIpPort) {
        logInfo(s"Updating %s to H2O's leader node %s".format(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, leaderIpPort))
        conf.setH2OCluster(leaderIpPort)
      }
      nodes
    } catch {
      case cause: RestApiException =>
        val h2oCluster = conf.h2oCluster.get + conf.contextPath.getOrElse("")
        if (conf.isAutoClusterStartUsed) {
          stopExternalH2OCluster(conf)
        }
        throw new H2OClusterNotReachableException(
          s"""External H2O cluster $h2oCluster - ${conf.cloudName.get} is not reachable.
             |H2OContext has not been created.""".stripMargin, cause)
    }
  }

  private def stopExternalH2OCluster(conf: H2OConf): Unit = {
    val yarnAppId = conf.getOption(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_YARN_APP_ID._1)
    ShellUtils.launchShellCommand(Seq[String]("yarn", "application", "-kill", yarnAppId.get))
  }

  private def verifyWebOpen(nodes: Array[NodeDesc], conf: H2OConf): Unit = {
    val nodesWithoutWeb = nodes.flatMap { node =>
      try {
        RestApiUtils.getCloudInfoFromNode(node, conf)
        None
      } catch {
        case cause: RestApiException => Some((node, cause))
      }
    }
    if (nodesWithoutWeb.nonEmpty) {
      throw new H2OClusterNotReachableException(
        s"""
    The following worker nodes are not reachable, but belong to the cluster:
    ${conf.h2oCluster.get} - ${conf.cloudName.get}:
    ----------------------------------------------
    ${nodesWithoutWeb.map(_._1.ipPort()).mkString("\n    ")}""", nodesWithoutWeb.head._2)
    }
  }

  private def verifyVersion(conf: H2OConf, nodes: Array[NodeDesc]): Unit = {
    val referencedVersion = BuildInfo.H2OVersion
    for (node <- nodes) {
      val externalVersion = RestApiUtils.getCloudInfoFromNode(node, conf).version
      if (referencedVersion != externalVersion) {
        if (conf.isAutoClusterStartUsed) {
          stopExternalH2OCluster(conf)
        }
        throw new RuntimeException(
          s"""The H2O node ${node.ipPort()} is of version $externalVersion but Sparkling Water
             |is using version of H2O $referencedVersion. Please make sure to use the corresponding assembly H2O JAR.""".stripMargin)
      }
    }
  }

  private def lockCloud(conf: H2OConf): Unit = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    RestApiUtils.update[CloudLockV3](endpoint, "/3/CloudLock", conf, Map("reason" -> "Locked from Sparkling Water."))
  }
}
