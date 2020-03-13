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

package ai.h2o.sparkling.backend.external

import java.io.{File, FileInputStream, FileOutputStream}
import java.net.{InetAddress, NetworkInterface}
import java.util.Properties

import ai.h2o.sparkling.backend.utils.{ArgumentBuilder, RestApiUtils, SharedBackendUtils, ShellUtils}
import ai.h2o.sparkling.backend.{SharedBackendConf, SparklingBackend}
import ai.h2o.sparkling.utils.ScalaUtils._
import ai.h2o.sparkling.utils.SparkSessionUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.{SparkEnv, SparkFiles}
import water.init.{HostnameGuesser, NetworkBridge}

import scala.io.Source

class ExternalH2OBackend(val hc: H2OContext) extends SparklingBackend with Logging {

  override def startH2OCluster(conf: H2OConf): Unit = {
    if (conf.isAutoClusterStartUsed) {
      launchExternalH2OOnYarn(conf)
    }
  }

  private def getYarnAppId(): Option[String] = {
    hc.getConf.getOption(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_YARN_APP_ID._1)
  }

  override def backendUIInfo: Seq[(String, String)] = {
    Seq(
      ("External backend YARN AppID", getYarnAppId()),
      ("External IP", hc.getConf.h2oCluster)
    ).filter(_._2.nonEmpty).map { case (k, v) => (k, v.get) }
  }

  override def epilog: String =
    if (hc.getConf.isAutoClusterStartUsed) {
      s"""
         | * Yarn App ID of external H2O cluster: ${getYarnAppId().get}
    """.stripMargin
    } else {
      ""
    }

  private def launchExternalH2OOnYarn(conf: H2OConf): Unit = {
    logInfo("Starting the external H2O cluster on YARN.")
    val cmdToLaunch = getExternalH2ONodesArguments(conf)
    logInfo("Command used to start H2O on yarn: " + cmdToLaunch.mkString(" "))

    val proc = ShellUtils.launchShellCommand(cmdToLaunch)

    val notifyFile = new File(conf.clusterInfoFile.get)
    if (!notifyFile.exists()) {
      throw new RuntimeException(
        s"""
           |Cluster notification file ${notifyFile.getAbsolutePath} could not be created. The possible causes are:
           |
           |1) External H2O cluster did not cloud within the pre-defined timeout. In that case, please try
           |   to increase the timeout for starting the external cluster as:
           |
           |   H2OContext.getOrCrete(H2OConf().setClusterStartTimeout(timeout))
           |
           |2) The file could not be created because of missing write rights.""".stripMargin
      )
    }
    // get ip port
    val clusterInfo = Source.fromFile(conf.clusterInfoFile.get).getLines()
    val ipPort = clusterInfo.next()
    val yarnAppId = clusterInfo.next().replace("job", "application")
    conf.set(ExternalBackendConf.PROP_EXTERNAL_CLUSTER_YARN_APP_ID._1, yarnAppId)
    // we no longer need the notification file
    new File(conf.clusterInfoFile.get).delete()
    logInfo(s"Yarn ID obtained from cluster file: $yarnAppId")
    logInfo(s"Cluster ip and port obtained from cluster file: $ipPort")

    assert(proc == 0, s"Starting external H2O cluster failed with return value $proc.")
    conf.setH2OCluster(ipPort)
  }

  private def getExternalH2ONodesArguments(conf: H2OConf): Seq[String] = {
    // Application tags shown in Yarn Resource Manager UI
    val yarnAppTags = s"${ExternalH2OBackend.TAG_EXTERNAL_H2O},${ExternalH2OBackend.TAG_SPARK_APP.format(hc.sparkContext.applicationId)}"
    new ArgumentBuilder()
      .add(Seq(conf.externalHadoopExecutable, "jar", conf.h2oDriverPath.get))
      .add("-libjars", getExtensionsAssemblyJar().getAbsolutePath + conf.externalExtraJars.map("," + _).getOrElse(""))
      .add("-files", getSecurityFiles(conf))
      .add(conf.YARNQueue.map(queue => s"-Dmapreduce.job.queuename=$queue"))
      .add(s"-Dmapreduce.job.tags=$yarnAppTags")
      .add(s"-Dai.h2o.args.config=sparkling-water-external") // H2O custom application master
      .add("-nodes", conf.clusterSize)
      .add("-notify", conf.clusterInfoFile)
      .add("-jobname", conf.cloudName)
      .add("-mapperXmx", conf.mapperXmx)
      .add("-nthreads", conf.nthreads)
      .add(Seq("-J", "-log_level", "-J", conf.h2oNodeLogLevel))
      .add("-port_offset", conf.internalPortOffset)
      .add("-baseport", conf.nodeBasePort)
      .add("-timeout", conf.clusterStartTimeout)
      .add("-disown")
      .add("-sw_ext_backend", !RestApiUtils.isRestAPIBased(hc))
      .add(Seq("-J", "-rest_api_ping_timeout", "-J", conf.clientCheckRetryTimeout.toString))
      .add(Seq("-J", "-client_disconnect_timeout", "-J", conf.clientCheckRetryTimeout.toString), !RestApiUtils.isRestAPIBased(hc))
      .add("-run_as_user", conf.runAsUser)
      .add(Seq("-J", "-stacktrace_collector_interval", "-J", conf.stacktraceCollectorInterval.toString), conf.stacktraceCollectorInterval != -1)
      .add("-output", conf.HDFSOutputDir)
      .add("-context_path", conf.contextPath)
      .add("-network", conf.nodeNetworkMask)
      .add(ExternalH2OBackend.getH2OSecurityArgs(conf))
      .add("-principal", conf.kerberosPrincipal)
      .add("-keytab", conf.kerberosKeytab)
      .add("-driverif", conf.externalH2ODriverIf)
      .add("-driverport", conf.externalH2ODriverPort)
      .add("-driverportrange", conf.externalH2ODriverPortRange)
      .add("-extramempercent", conf.externalExtraMemoryPercent)
      .add(conf.nodeExtraProperties)
      .add(ExternalH2OBackend.getExtraHttpHeaderArgs(conf).flatMap(arg => Seq("-J", arg)))
      .buildArgs()
  }

  private def getExtensionsAssemblyJar(): File = {
    val fileInJar = "assembly-extensions.jar.embedded"
    val tempFile = File.createTempFile("assembly-extensions-", ".jar")
    tempFile.deleteOnExit()
    withResource(new FileOutputStream(tempFile)) { outputStream =>
      withResource(getClass.getClassLoader.getResourceAsStream(fileInJar)) { inputStream =>
        IOUtils.copy(inputStream, outputStream)
      }
    }
    tempFile
  }

  private def getSecurityFiles(conf: H2OConf): Option[String] = {
    conf.sslConf match {
      case Some(internalSecurityConf) =>
        // In External Backend, auto mode we need distribute the keystore files to the H2O cluster
        val props = new Properties()
        props.load(new FileInputStream(internalSecurityConf))
        val keyStoreFiles = Array(props.get("h2o_ssl_jks_internal"), props.get("h2o_ssl_jts")).map(f => SparkFiles.get(f.asInstanceOf[String]))
        logInfo(s"Starting external H2O cluster in encrypted mode with config: $internalSecurityConf")
        Some(keyStoreFiles.mkString(","))
      case _ =>
        None
    }
  }
}

object ExternalH2OBackend extends SharedBackendUtils {

  override def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    super.checkAndUpdateConf(conf)
    setClientIp(conf)
    // Increase locality timeout since h2o-specific tasks can be long computing
    if (conf.getInt("spark.locality.wait", 3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }

    if (conf.clusterStartMode != ExternalBackendConf.EXTERNAL_BACKEND_MANUAL_MODE &&
      conf.clusterStartMode != ExternalBackendConf.EXTERNAL_BACKEND_AUTO_MODE) {

      throw new IllegalArgumentException(
        s"""'${ExternalBackendConf.PROP_EXTERNAL_CLUSTER_START_MODE._1}' property is set to ${conf.clusterStartMode}.
          Valid options are "${ExternalBackendConf.EXTERNAL_BACKEND_MANUAL_MODE}" or "${ExternalBackendConf.EXTERNAL_BACKEND_AUTO_MODE}".
      """)
    }

    if (conf.isAutoClusterStartUsed) {
      val envDriverJar = ExternalH2OBackend.ENV_H2O_DRIVER_JAR

      lazy val driverPath = sys.env.get(envDriverJar)
      if (conf.h2oDriverPath.isEmpty && driverPath.isEmpty) {
        throw new IllegalArgumentException(
          s"""Path to the H2O driver has to be specified when using automatic cluster start.
             |It can be specified either via method available on the configuration object or
             |by using the '$envDriverJar' environmental property.
          """.stripMargin)
      }

      if (conf.h2oDriverPath.isEmpty && driverPath.isDefined) {
        logInfo(
          s"""Obtaining path to the H2O driver from the environment variable $envDriverJar.
             |Specified path is: ${driverPath.get}""".stripMargin)
        conf.setH2ODriverPath(driverPath.get)
      }

      if (conf.clientCheckRetryTimeout < conf.backendHeartbeatInterval) {
        logWarning(s"%s needs to be larger than %s, increasing the value to %d".format(
          SharedBackendConf.PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1,
          SharedBackendConf.PROP_BACKEND_HEARTBEAT_INTERVAL._1,
          conf.backendHeartbeatInterval * 6
        ))
        conf.setClientCheckRetryTimeout(conf.backendHeartbeatInterval * 6)
      }

      if (conf.clusterSize.isEmpty) {
        throw new IllegalArgumentException("Cluster size of external H2O cluster has to be specified in automatic mode of external H2O backend!")
      }

      if (conf.cloudName.isEmpty) {
        conf.setCloudName(H2O_JOB_NAME.format(SparkSessionUtils.active.sparkContext.applicationId))
      }

      if (conf.clusterInfoFile.isEmpty) {
        conf.setClusterInfoFile("notify_" + conf.cloudName.get)
      }

      if (conf.getOption("spark.yarn.principal").isDefined &&
        conf.kerberosPrincipal.isEmpty) {
        logInfo(s"spark.yarn.principal provided and ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_PRINCIPAL._1} is" +
          s" not set. Passing the configuration to H2O.")
        conf.setKerberosPrincipal(conf.get("spark.yarn.principal"))
      }

      if (conf.getOption("spark.yarn.keytab").isDefined &&
        conf.kerberosKeytab.isEmpty) {
        logInfo(s"spark.yarn.keytab provided and ${ExternalBackendConf.PROP_EXTERNAL_KERBEROS_KEYTAB._1} is" +
          s" not set. Passing the configuration to H2O.")
        conf.setKerberosKeytab(conf.get("spark.yarn.keytab"))
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
    distributeFiles(conf, SparkSessionUtils.active.sparkContext)
    conf
  }

  // This string tags instances of H2O launched from Sparkling Water
  val TAG_EXTERNAL_H2O = "H2O/Sparkling-Water"

  // Another tag which identifies launcher - aka Spark application
  val TAG_SPARK_APP = "Sparkling-Water/Spark/%s"

  // Job name for H2O Yarn job
  val H2O_JOB_NAME = "H2O_via_SparklingWater_%s"

  // Name of the environmental property, which may contain path to the external H2O driver
  val ENV_H2O_DRIVER_JAR = "H2O_DRIVER_JAR"

  private def setClientIp(conf: H2OConf): Unit = {
    val clientIp = identifyClientIp(conf.h2oClusterHost.get)
    if (clientIp.isDefined && conf.clientNetworkMask.isEmpty) {
      conf.setClientIp(clientIp.get)
    }

    if (conf.clientIp.isEmpty) {
      conf.setClientIp(ExternalH2OBackend.getHostname(SparkEnv.get))
    }
  }

  private def identifyClientIp(remoteAddress: String): Option[String] = {
    val interfaces = NetworkInterface.getNetworkInterfaces
    while (interfaces.hasMoreElements) {
      val interface = interfaces.nextElement()
      import scala.collection.JavaConverters._
      interface.getInterfaceAddresses.asScala.foreach { address =>
        val ip = address.getAddress.getHostAddress + "/" + address.getNetworkPrefixLength
        val cidr = HostnameGuesser.CIDRBlock.parse(ip)
        if (cidr != null && NetworkBridge.isInetAddressOnNetwork(cidr, InetAddress.getByName(remoteAddress))) {
          return Some(address.getAddress.getHostAddress)
        }
      }
    }
    None
  }
}
