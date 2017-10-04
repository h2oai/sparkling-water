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

package org.apache.spark.h2o.backends.external


import java.io.{File, FileInputStream}
import java.util.Properties

import org.apache.spark.h2o.backends.external.ExternalH2OBackend.H2O_JOB_NAME
import org.apache.spark.h2o.backends.{SharedBackendConf, SparklingBackend}
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.internal.Logging
import water.api.RestAPIManager
import water.{H2O, H2OStarter}

import scala.io.Source
import scala.util.control.NoStackTrace


class ExternalH2OBackend(val hc: H2OContext) extends SparklingBackend with ExternalBackendUtils with Logging {

  private var yarnAppId: Option[String] = None
  private var externalIP: Option[String] = None

  def launchH2OOnYarn(conf: H2OConf): String = {
    import ExternalH2OBackend._

    var cmdToLaunch = Seq[String]("hadoop", "jar", conf.h2oDriverPath.get)

    conf.sslConf match {
      case Some(ssl) =>
        val sslConfig = new Properties()
        sslConfig.load(new FileInputStream(ssl))
        cmdToLaunch = cmdToLaunch ++ Array("-files", sslConfig.get("h2o_ssl_jks_internal") + "," + sslConfig.get("h2o_ssl_jts"))
        cmdToLaunch = cmdToLaunch ++ Array("-internal_security", ssl)
        logInfo(s"Running external H2O cluster in encrypted mode with config: $ssl")
      case _ =>
    }
    // Application tags shown in Yarn Resource Manager UI
    val yarnAppTags = s"${TAG_EXTERNAL_H2O},${TAG_SPARK_APP.format(hc.sparkContext.applicationId)}"

    cmdToLaunch = cmdToLaunch ++ Seq[String](
      conf.YARNQueue.map("-Dmapreduce.job.queuename=" + _).getOrElse(""),
      s"-Dmapreduce.job.tags=${yarnAppTags}",
      "-Dmapreduce.framework.name=h2o-yarn", // use H2O's custom application Master
      "-nodes", conf.numOfExternalH2ONodes.get,
      "-notify", conf.clusterInfoFile.get,
      "-J", "-md5skip",
      "-jobname", conf.cloudName.get,
      "-mapperXmx", conf.mapperXmx,
      "-output", conf.HDFSOutputDir.get,
      "-J", "-log_level", "-J", conf.h2oNodeLogLevel,
      "-timeout", conf.clusterStartTimeout.toString,
      "-disown",
      "-J", "-watchdog_stop_without_client",
      "-J", "-watchdog_client_connect_timeout", "-J", conf.clientConnectionTimeout.toString,
      "-J", "-watchdog_client_retry_timeout", "-J", conf.clientCheckRetryTimeout.toString
    )

    // start external H2O cluster and log the output
    logInfo("Command used to start H2O on yarn: " + cmdToLaunch.mkString(" "))

    import scala.sys.process._
    val processOut = new StringBuffer()
    val processErr = new StringBuffer()

    val proc = cmdToLaunch.mkString(" ").!(ProcessLogger(
      { msg =>
        processOut.append(msg + "\n")
        println(msg)
      }, {
        errMsg =>
          processErr.append(errMsg + "\n")
          println(errMsg)
      }))

    logInfo(processOut.toString)
    logError(processErr.toString)

    val notifFile = new File(hc.getConf.clusterInfoFile.get)
    if (!notifFile.exists()) {
      throw new RuntimeException(
        s"""
           |Cluster notification file ${notifFile.getAbsolutePath} could not be created. The possible causes are:
           |
          |1) External H2O cluster did not cloud within the pre-defined timeout. In that case, please try
           |   to increase the timeout for starting the external cluster as:
           |   Python: H2OConf(sc).set_cluster_start_timeout(timeout)....
           |   Scala:  new H2OConf(sc).setClusterStartTimeout(timeout)....
           |
          |2) The file could not be created because of missing write rights.""".stripMargin
      )
    }
    // get ip port
    val clusterInfo = Source.fromFile(hc.getConf.clusterInfoFile.get).getLines
    val ipPort = clusterInfo.next()
    yarnAppId = Some(clusterInfo.next().replace("job", "application"))
    externalIP = Some(ipPort)
    // we no longer need the notification file
    new File(hc.getConf.clusterInfoFile.get).delete()
    logInfo(s"Yarn ID obtained from cluster file: $yarnAppId")
    logInfo(s"Cluster ip and port obtained from cluster file: $ipPort")

    assert(proc == 0, s"Starting external H2O cluster failed with return value $proc.")
    ipPort
  }
  
  override def init(): Array[NodeDesc] = {
    if (hc.getConf.isAutoClusterStartUsed) {
      // start h2o instances on yarn
      logInfo("Starting the external H2O cluster on YARN.")
      val ipPort = launchH2OOnYarn(hc.getConf)
      hc._conf.setH2OCluster(ipPort)
    }
    logTrace("Starting H2O client node and connecting to external H2O cluster.")

    val h2oClientArgs = if (hc.getConf.isAutoClusterStartUsed) {
      getH2OClientArgs(hc.getConf) ++ Array("-watchdog_client")
    } else {
      getH2OClientArgs(hc.getConf)
    }
    logDebug(s"Arguments used for launching the H2O client node: ${h2oClientArgs.mkString(" ")}")
    H2OStarter.start(h2oClientArgs, false)

    if (hc.getConf.numOfExternalH2ONodes.isDefined) {
      H2O.waitForCloudSize(hc.getConf.numOfExternalH2ONodes.get.toInt, hc.getConf.cloudTimeout)
    }
    // Register web API for client
    RestAPIManager(hc).registerAll()
    Thread.sleep(2000)
    H2O.startServingRestApi()

    if (cloudMembers.length == 0) {
      if (hc.getConf.isManualClusterStartUsed) {
        throw new H2OClusterNotRunning(
          s"""
             |External H2O cluster is not running or could not be connected to. Provided configuration:
             |  cluster name            : ${hc.getConf.cloudName.get}
             |  cluster representative  : ${hc.getConf.h2oCluster.getOrElse("Not set, using multi-cast!")}
             |  cluster start timeout   : ${hc.getConf.clusterStartTimeout} seconds"
             |
             |It is possible that in case you provided only the cluster name, h2o is not able to cloud up
             |because multi-cast communication is limited in your network. In that case, please consider starting the
             |external H2O cluster with flatfile and set the following configuration '${
            ExternalBackendConf.
              PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1
          }'
        """.stripMargin)
      } else {
        throw new H2OClusterNotRunning("Problem with connecting to external H2O cluster started on yarn." +
          "Please check the YARN logs.")
      }
    }
    cloudMembers
  }

  override def backendUIInfo: Seq[(String, String)] = {
    Seq(
      ("External backend YARN AppID", yarnAppId),
      ("External IP", externalIP)
    ).filter(_._2.nonEmpty).map { case (k, v) => (k, v.get) }
  }

  override def stop(stopSparkContext: Boolean): Unit = {
    if (stopSparkContext) {
      hc.sparkContext.stop()
    }
    H2O.orderlyShutdown(1000)
    H2O.exit(0)
  }

  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)


    if (conf.isAutoClusterStartUsed) {
      lazy val driverPath = sys.env.get(ExternalH2OBackend.ENV_H2O_EXTENDED_JAR)
      if (conf.h2oDriverPath.isEmpty && driverPath.isEmpty) {
        throw new IllegalArgumentException(
          s"""Path to the H2O extended driver has to be specified when using automatic cluster start.
             |It can be specified either via method available on the configuration object or
             |using the '${ExternalH2OBackend.ENV_H2O_EXTENDED_JAR}' environmental property.
          """.stripMargin)
      }
      if (conf.h2oDriverPath.isEmpty && driverPath.isDefined) {
        log.info(
          s"""Obtaining path to the extended H2O driver from the environment variable.
             |Specified path is: ${driverPath.get}""".stripMargin)
        conf.setH2ODriverPath(driverPath.get)
      }

      if (conf.cloudName.isEmpty) {
        conf.setCloudName(H2O_JOB_NAME.format(hc.sparkContext.applicationId))
      }

      if (conf.numOfExternalH2ONodes.isEmpty) {
        throw new IllegalArgumentException("Number of external H2O nodes has to be specified in the auto H2O external start mode!")
      }

      if (conf.HDFSOutputDir.isEmpty) {
        conf.setHDFSOutputDir(conf.cloudName.get)
      }

      if (conf.clusterInfoFile.isEmpty) {
        conf.setClusterConfigFile("notify_" + conf.cloudName.get)
      }

    } else {
      if (conf.cloudName.isEmpty) {
        throw new IllegalArgumentException(
          s"""Cluster name has to be specified when using the external H2O cluster mode in the manual start mode.
             |It can be set either on the configuration object or via '${SharedBackendConf.PROP_CLOUD_NAME._1}'
             |spark configuration property""".stripMargin)
      }
    }
    conf
  }
}

object ExternalH2OBackend {
  // This string tags instances of H2O launched from Sparkling Water
  val TAG_EXTERNAL_H2O = "H2O/Sparkling-Water"
  // Another tag which identifies launcher - aka Spark application
  val TAG_SPARK_APP = "Sparkling-Water/Spark/%s"
  // Job name for H2O Yarn job
  val H2O_JOB_NAME = "H2O_via_SparklingWater_%s"

  // Name of the environmental property, which may contain path to the external H2O driver
  val ENV_H2O_EXTENDED_JAR = "H2O_EXTENDED_JAR"
}

class H2OClusterNotRunning(msg: String) extends Exception(msg) with NoStackTrace
