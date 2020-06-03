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

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import ai.h2o.sparkling.backend.exceptions.{H2OClusterNotReachableException, RestApiCommunicationException, RestApiException}
import ai.h2o.sparkling.backend.external.ExternalBackendConf
import ai.h2o.sparkling.backend.{BuildInfo, H2OJob, NodeDesc}
import ai.h2o.sparkling.extensions.rest.api.schema.{VerifyVersionV3, VerifyWebOpenV3}
import ai.h2o.sparkling.{H2OConf, H2OContext, H2OFrame}
import org.apache.spark.SparkContext
import water.api.ImportHiveTableHandler.HiveTableImporter
import water.api.schemas3.{CloudLockV3, JobV3}

trait H2OContextExtensions extends RestCommunication with RestApiUtils with ShellUtils {
  _: H2OContext =>

  def downloadH2OLogs(destinationDir: String, logContainer: String): String = {
    verifyLogContainer(logContainer)
    val endpoint = RestApiUtils.getClusterEndpoint(getConf)
    val file = new File(destinationDir, s"${logFileName()}.${logContainer.toLowerCase}")
    val logEndpoint = s"/3/Logs/download/$logContainer"
    logContainer match {
      case "LOG" =>
        downloadStringURLContent(endpoint, logEndpoint, getConf, file)
      case "ZIP" =>
        downloadBinaryURLContent(endpoint, logEndpoint, getConf, file)
    }
    file.getAbsolutePath
  }

  def importHiveTable(
      database: String = HiveTableImporter.DEFAULT_DATABASE,
      table: String,
      partitions: Array[Array[String]] = null,
      allowMultiFormat: Boolean = false): H2OFrame = {
    val endpoint = RestApiUtils.getClusterEndpoint(getConf)
    val params = Map(
      "database" -> database,
      "table" -> table,
      "partitions" -> partitions,
      "allow_multi_format" -> allowMultiFormat)
    try {
      val job = RestApiUtils.update[JobV3](endpoint, "/3/ImportHiveTable", getConf, params)
      H2OJob(job.key.name).waitForFinish()
      H2OFrame(job.dest.name)
    } catch {
      case e: RestApiCommunicationException if e.getMessage.contains("table not found") =>
        throw new IllegalArgumentException(s"Table '${table}' not found in the Hive database '${database}'!")
    }
  }

  protected def withConversionDebugPrints(sc: SparkContext, conversionName: String, block: => H2OFrame): H2OFrame = {
    val propName = "spark.h2o.measurements.timing"
    val performancePrintConf = sc.getConf.getOption(propName).orElse(sys.props.get(propName))

    if (performancePrintConf.nonEmpty && performancePrintConf.get.toBoolean) {
      val t0 = System.nanoTime()
      val result = block
      val t1 = System.nanoTime()
      logInfo(
        s"Elapsed time of the $conversionName conversion into H2OFrame ${result.frameId}: " + (t1 - t0) / 1000 + " millis")
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

  private def logFileName(): String = {
    val pattern = "yyyyMMdd_hhmmss"
    val formatter = new SimpleDateFormat(pattern)
    val now = formatter.format(new Date)
    s"h2ologs_$now"
  }

  private def verifyLogContainer(logContainer: String): Unit = {
    if (!Seq("ZIP", "LOG").contains(logContainer)) {
      throw new IllegalArgumentException(s"Supported LOG container is either LOG or ZIP, specified was: $logContainer")
    }
  }

  protected def verifyWorkerNodes(conf: H2OConf): Unit = {
    try {
      lockCloud(conf)
      verifyWebOpen(conf)
      if (!conf.isBackendVersionCheckDisabled) {
        verifyVersion(conf)
      }
      val leaderIpPort = RestApiUtils.getLeaderNode(conf).ipPort()
      if (conf.h2oCluster.get != leaderIpPort) {
        logInfo(
          s"Updating %s to H2O's leader node %s"
            .format(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, leaderIpPort))
        conf.setH2OCluster(leaderIpPort)
      }
    } catch {
      case cause: RestApiException =>
        val h2oCluster = conf.h2oCluster.get + conf.contextPath.getOrElse("")
        if (conf.isAutoClusterStartUsed) {
          stopExternalH2OCluster(conf)
        }
        throw new H2OClusterNotReachableException(
          s"""External H2O cluster $h2oCluster - ${conf.cloudName.get} is not reachable.
             |H2OContext has not been created.""".stripMargin,
          cause)
    }
  }

  private def stopExternalH2OCluster(conf: H2OConf): Unit = {
    val yarnAppId = conf.getOption(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_YARN_APP_ID._1)
    launchShellCommand(Seq[String]("yarn", "application", "-kill", yarnAppId.get))
  }

  private def verifyWebOpen(conf: H2OConf): Unit = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val verifyWebOpenV3 = RestApiUtils.query[VerifyWebOpenV3](endpoint, "/3/verifyWebOpen", conf)
    val nodesWithoutWeb = verifyWebOpenV3.nodes_web_disabled
    if (nodesWithoutWeb.nonEmpty) {
      throw new H2OClusterNotReachableException(s"""
    The following worker nodes are not reachable, but belong to the cluster:
    ${conf.h2oCluster.get} - ${conf.cloudName.get}:
    ----------------------------------------------
    ${nodesWithoutWeb.mkString("\n    ")}""".stripMargin)
    }
  }

  private def verifyVersion(conf: H2OConf): Unit = {
    val referencedVersion = BuildInfo.H2OVersion
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val params = Map("referenced_version" -> referencedVersion)
    val verifyVersionV3 = RestApiUtils.query[VerifyVersionV3](endpoint, "/3/verifyVersion", conf, params)
    val nodesWrongVersion = verifyVersionV3.nodes_wrong_version.zip(verifyVersionV3.versions)
    if (nodesWrongVersion.nonEmpty) {
      if (conf.isAutoClusterStartUsed) {
        stopExternalH2OCluster(conf)
      }
      throw new RuntimeException(s"""
    Sparkling Water is using version of H2O $referencedVersion, but the following nodes have different version:
    ----------------------------------------------
    ${nodesWrongVersion.map(info => info._1 + " - " + info._2).mkString("\n    ")}

    Please make sure to use the corresponding assembly H2O JAR.""".stripMargin)
    }
  }

  private def lockCloud(conf: H2OConf): Unit = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    RestApiUtils.update[CloudLockV3](endpoint, "/3/CloudLock", conf, Map("reason" -> "Locked from Sparkling Water."))
  }
}
