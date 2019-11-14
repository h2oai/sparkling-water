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

import org.apache.spark.SparkEnv
import org.apache.spark.h2o.backends.external.ExternalH2OBackend.H2O_JOB_NAME
import org.apache.spark.h2o.backends.{SharedBackendConf, SparklingBackend}
import org.apache.spark.h2o.utils.{H2OClusterNodeNotReachableException, H2OContextRestAPIUtils, NodeDesc}
import org.apache.spark.h2o.{BuildInfo, H2OConf, H2OContext}
import org.apache.spark.internal.Logging
import water.api.RestAPIManager
import water.init.NetworkUtils
import water.util.Log
import water.{H2O, H2OStarter}

import scala.io.Source
import scala.util.control.NoStackTrace


class ExternalH2OBackend(val hc: H2OContext) extends SparklingBackend with ExternalBackendUtils with Logging with H2OContextRestAPIUtils {

  var yarnAppId: Option[String] = None
  private var externalIP: Option[String] = None
  private var cloudHealthCheckKillThread: Option[Thread] = None
  private var cloudHealthCheckThread: Option[Thread] = None

  private def runningFromNonJVMClient(hc: H2OContext): Boolean = {
    hc._conf.getBoolean(SharedBackendConf.PROP_RUNNING_FROM_NON_JVM_CLIENT._1,
      SharedBackendConf.PROP_RUNNING_FROM_NON_JVM_CLIENT._2)
  }

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

    if (conf.YARNQueue.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-Dmapreduce.job.queuename=" + conf.YARNQueue.get)
    }

    cmdToLaunch = cmdToLaunch ++ Seq[String](
      s"-Dmapreduce.job.tags=${yarnAppTags}",
      s"-Dai.h2o.args.config=sparkling-water-external",
      "-Dmapreduce.framework.name=h2o-yarn", // use H2O's custom application Master
      "-nodes", conf.clusterSize.get,
      "-notify", conf.clusterInfoFile.get,
      "-jobname", conf.cloudName.get,
      "-mapperXmx", conf.mapperXmx,
      "-nthreads", conf.nthreads.toString,
      "-J", "-log_level", "-J", conf.h2oNodeLogLevel,
      "-port_offset", conf.internalPortOffset.toString,
      "-baseport", conf.nodeBasePort.toString,
      "-timeout", conf.clusterStartTimeout.toString,
      "-disown",
      "-J", "-client_disconnect_timeout", "-J", conf.clientCheckRetryTimeout.toString,
      "-J", "-watchdog_stop_without_client",
      "-J", "-watchdog_client_connect_timeout", "-J", conf.clientConnectionTimeout.toString,
      "-J", "-watchdog_client_retry_timeout", "-J", conf.clientCheckRetryTimeout.toString
    )

    if (conf.runAsUser.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-run_as_user", conf.runAsUser.get)
    }

    if (conf.stacktraceCollectorInterval != -1) { // -1 means don't do stacktrace collection
      cmdToLaunch = cmdToLaunch ++ Seq[String]("-J", "-stacktrace_collector_interval", "-J", conf.stacktraceCollectorInterval.toString)
    }

    if (conf.HDFSOutputDir.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq[String]("-output", conf.HDFSOutputDir.get)
    }

    if (conf.h2oDriverIf.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq[String]("-driverif", conf.h2oDriverIf.get)
    }

    if (hc.getConf.contextPath.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-context_path", hc.getConf.contextPath.get)
    }

    if (hc.getConf.nodeNetworkMask.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-network", hc.getConf.nodeNetworkMask.get)
    }

    cmdToLaunch = cmdToLaunch ++ getH2OSecurityArgs(hc.getConf)

    if (hc.getConf.kerberosKeytab.isDefined && hc.getConf.kerberosPrincipal.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-principal",
        hc.getConf.kerberosPrincipal.get, "-keytab", hc.getConf.kerberosKeytab.get)
    }

    if (conf.externalH2ODriverIf.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-driverif", conf.externalH2ODriverIf.get)
    }

    if (conf.externalH2ODriverPort.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-driverport", conf.externalH2ODriverPort.get)
    }

    if (conf.externalH2ODriverPortRange.isDefined) {
      cmdToLaunch = cmdToLaunch ++ Seq("-driverportrange", conf.externalH2ODriverPortRange.get)
    }

    cmdToLaunch = cmdToLaunch ++ Seq("-extramempercent", conf.externalExtraMemoryPercent.toString)

    if (conf.nodeExtraProperties.isDefined) {
      cmdToLaunch = cmdToLaunch :+ conf.nodeExtraProperties.get
    }

    cmdToLaunch = cmdToLaunch ++ getExtraHttpHeaderArgs(conf).flatMap(arg => Seq("-J", arg))

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
      val clientIp = NetworkUtils.indentifyClientIp(ipPort.split(":")(0))
      if (clientIp.isDefined && hc._conf.clientIp.isEmpty && hc._conf.clientNetworkMask.isEmpty) {
        hc._conf.setClientIp(clientIp.get)
      }
    } else {
        val clientIp = NetworkUtils.indentifyClientIp(hc._conf.h2oClusterHost.get)
        if (clientIp.isDefined && hc._conf.clientIp.isEmpty && hc._conf.clientNetworkMask.isEmpty) {
          hc._conf.setClientIp(clientIp.get)
        }
    }

    if (hc._conf.clientIp.isEmpty) {
      hc._conf.setClientIp(getHostname(SparkEnv.get))
    }

    logInfo("Connecting to external H2O cluster.")
    val clusterBuildTimeout = hc.getConf.cloudTimeout
    val nodes = if (runningFromNonJVMClient(hc)) {
      try {
        val nodes = getNodes(hc.getConf)
        verifyWebOpen(nodes, hc.getConf)
        nodes
      } catch {
        case _: H2OClusterNodeNotReachableException =>
          val h2oCluster = hc.getConf.h2oCluster.get + hc.getConf.contextPath.getOrElse("")
          throw new H2OClusterNodeNotReachableException(
            s"""External H2O cluster $h2oCluster - ${hc.getConf.cloudName.get} is not reachable, H2OContext has not been created.
               |Please verify that $h2oCluster is running with web enabled and retry the context creation.
               |If your cluster is secured, also make sure you that are providing valid credentials to the client. """.stripMargin)
      }
    } else {
      val h2oClientArgs = getH2OClientArgs(hc.getConf).toArray
      logDebug(s"Arguments used for launching the H2O client node: ${h2oClientArgs.mkString(" ")}")

    H2OStarter.start(h2oClientArgs, false)

    val expectedSize = hc.getConf.clusterSize.get.toInt
    val discoveredSize = waitForCloudSize(expectedSize, clusterBuildTimeout)
    if (discoveredSize < expectedSize) {
      if (hc.getConf.isAutoClusterStartUsed) {
        Log.err(s"Exiting! External H2O cluster was of size $discoveredSize but expected was $expectedSize!!")
        H2O.shutdown(-1)
      }
      throw new RuntimeException("Cloud size " + discoveredSize + " under " + expectedSize);
    }

      // Register web API for client
      RestAPIManager(hc).registerAll()
      H2O.startServingRestApi()
      val cloudMembers = H2O.CLOUD.members().map(NodeDesc(_))
      if (cloudMembers.length == 0) {
        if (hc.getConf.isManualClusterStartUsed) {
          throw new H2OClusterNotRunning(
            s"""
               |External H2O cluster is not running or could not be connected to. Provided configuration:
               |  cluster name            : ${hc.getConf.cloudName.get}
               |  cluster representative  : ${hc.getConf.h2oCluster.getOrElse("Using multi-cast discovery!")}
               |  cluster start timeout   : ${hc.getConf.clusterStartTimeout} sec
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

      startUnhealthyStateKillThread()
      startUnhealthyStateCheckThread()

      cloudMembers
    }
    verifyVersionFromRestCall(nodes)
    nodes
  }

  def startUnhealthyStateKillThread(): Unit = {
    if (hc.getConf.isKillOnUnhealthyClusterEnabled) {
      cloudHealthCheckKillThread = Some(new Thread {
        override def run(): Unit = {
          while (true) {
            Thread.sleep(hc.getConf.killOnUnhealthyClusterInterval)
            if (!H2O.CLOUD.healthy() && hc.getConf.isKillOnUnhealthyClusterEnabled) {
              Log.err("Exiting! External H2O cluster not healthy!!")
              H2O.shutdown(-1)
            }
          }
        }
      })

      cloudHealthCheckKillThread.get.start()
    }
  }

  def startUnhealthyStateCheckThread(): Unit = {
    cloudHealthCheckThread = Some(new Thread {
      override def run(): Unit = {
        while (true) {
          Thread.sleep(hc.getConf.healthCheckInterval)
          if (!H2O.CLOUD.healthy()) {
            Log.err("External H2O cluster not healthy!!")
          }
        }
      }
    })
    cloudHealthCheckThread.get.start()
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

    // In Manual mode of external backend, we want the H2O cluster to be managed by the user, not by Sparkling Water
    if (hc._conf.isAutoClusterStartUsed) {
      H2O.orderlyShutdown(1000)
    }
    if (hc._conf.isManualClusterStartUsed && runningFromNonJVMClient(hc)) {
      // Do nothing, we don't have H2O client running, we do not have nothing to stop (and H2O.exit just kills the process)
    } else if (hc.sparkContext.conf.get("spark.submit.deployMode", "client") != "cluster") {
      // Stop h2o when running standalone pysparkling scripts, only in client deploy mode
      //, so the user does not need explicitly close h2o.
      // In driver mode the application would call exit which is handled by Spark AM as failure
      H2O.exit(0)
    }
  }

  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)

    if (conf.clusterStartMode != ExternalBackendConf.EXTERNAL_BACKEND_MANUAL_MODE &&
      conf.clusterStartMode != ExternalBackendConf.EXTERNAL_BACKEND_AUTO_MODE) {

      throw new IllegalArgumentException(
        s"""'${ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._1}' property is set to ${conf.clusterStartMode}.
          Valid options are "${ExternalBackendConf.EXTERNAL_BACKEND_MANUAL_MODE}" or "${ExternalBackendConf.EXTERNAL_BACKEND_AUTO_MODE}".
      """)
    }

    if (conf.clusterSize.isEmpty && !conf.getBoolean(SharedBackendConf.PROP_RUNNING_FROM_NON_JVM_CLIENT._1,
      SharedBackendConf.PROP_RUNNING_FROM_NON_JVM_CLIENT._2)) {
      throw new IllegalArgumentException("Cluster size of external H2O cluster has to be specified!")
    }

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

      if (conf.clusterInfoFile.isEmpty) {
        conf.setClusterConfigFile("notify_" + conf.cloudName.get)
      }

      if (hc.sparkContext.conf.getOption("spark.yarn.principal").isDefined &&
        conf.kerberosPrincipal.isEmpty) {
        Log.info(s"spark.yarn.principal provided and ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_PRINCIPAL._1} is" +
          s" not set. Passing the configuration to H2O.")
        conf.setKerberosPrincipal(hc.sparkContext.conf.get("spark.yarn.principal"))
      }

      if (hc.sparkContext.conf.getOption("spark.yarn.keytab").isDefined &&
        conf.kerberosKeytab.isEmpty) {
        Log.info(s"spark.yarn.keytab provided and ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_KEYTAB._1} is" +
          s" not set. Passing the configuration to H2O.")
        conf.setKerberosKeytab(hc.sparkContext.conf.get("spark.yarn.keytab"))
      }

      if (conf.kerberosKeytab.isDefined && conf.kerberosPrincipal.isEmpty) {
        throw new IllegalArgumentException(
          s"""
             |  Both options ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_KEYTAB._1} and
             |  ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_PRINCIPAL._1} need to be provided, specified has
             |  been just ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_KEYTAB._1}
          """.stripMargin)
      } else if (conf.kerberosPrincipal.isDefined && conf.kerberosKeytab.isEmpty) {
        throw new IllegalArgumentException(
          s"""
             |  Both options ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_KEYTAB._1} and
             |  ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_PRINCIPAL._1} need to be provided, specified has
             |  been just ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_PRINCIPAL._1}
          """.stripMargin)
      }
    } else {
      if (conf.cloudName.isEmpty) {
        throw new IllegalArgumentException(
          s"""Cluster name has to be specified when using the external H2O cluster mode in the manual start mode.
             |It can be set either on the configuration object or via '${SharedBackendConf.PROP_CLOUD_NAME._1}'
             |spark configuration property""".stripMargin)
      }

      if (conf.h2oCluster.isEmpty) {
        throw new IllegalArgumentException("H2O Cluster endpoint has to be specified!")
      }
    }
    distributeFiles(conf, hc.sparkContext)
    conf
  }

  override def epilog =
    if (hc._conf.isAutoClusterStartUsed) {
      s"""
         | * Yarn App ID of external H2O cluster: ${yarnAppId.get}
    """.stripMargin
    } else {
      ""
    }

  private def verifyVersionFromRestCall(nodes: Array[NodeDesc]) = {
    val referencedVersion = BuildInfo.H2OVersion
    for (node <- nodes) {
      val externalVersion = getCloudInfoFromNode(node, hc._conf).version
      if (referencedVersion != externalVersion) {
        throw new RuntimeException(
          s"""The external H2O node ${node.ipPort()} is of version $externalVersion but Sparkling Water
             |is using version of H2O $referencedVersion. Please make sure to use the corresponding assembly H2O JAR.""".stripMargin)
      }
    }
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
