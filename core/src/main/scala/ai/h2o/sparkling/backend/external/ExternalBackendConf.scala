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

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.utils.Compression
import org.apache.spark.expose.Logging
import org.apache.spark.h2o.H2OConf

import scala.collection.JavaConverters._

/**
 * External backend configuration
 */
trait ExternalBackendConf extends SharedBackendConf with Logging {
  self: H2OConf =>

  import ExternalBackendConf._

  /** Getters */

  def h2oCluster: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)

  def h2oClusterHost: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(0))

  def h2oClusterPort: Option[Int] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(1).toInt)

  def clusterSize: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_SIZE._1)

  def clusterStartTimeout: Int = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, PROP_EXTERNAL_CLUSTER_START_TIMEOUT._2)

  def clusterInfoFile: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_INFO_FILE._1)

  def mapperXmx: String = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)

  def HDFSOutputDir: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)

  def isAutoClusterStartUsed: Boolean = clusterStartMode == EXTERNAL_BACKEND_AUTO_MODE

  def isManualClusterStartUsed: Boolean = clusterStartMode == EXTERNAL_BACKEND_MANUAL_MODE

  def clusterStartMode: String = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)

  def h2oDriverPath: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)

  def YARNQueue: Option[String] = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)

  def isKillOnUnhealthyClusterEnabled: Boolean = sparkConf.getBoolean(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._2)

  def kerberosPrincipal: Option[String] = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1)

  def kerberosKeytab: Option[String] = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_KEYTAB._1)

  def runAsUser: Option[String] = sparkConf.getOption(PROP_EXTERNAL_RUN_AS_USER._1)

  def externalH2ODriverIf: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_IF._1)

  def externalH2ODriverPort: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT._1)

  def externalH2ODriverPortRange: Option[String] = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT_RANGE._1)

  def externalExtraMemoryPercent: Int = sparkConf.getInt(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._2)

  def externalBackendStopTimeout: Int = sparkConf.getInt(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._2)

  def externalHadoopExecutable: String = sparkConf.get(PROP_EXTERNAL_HADOOP_EXECUTABLE._1, PROP_EXTERNAL_HADOOP_EXECUTABLE._2)

  def externalExtraJars: Option[String] = sparkConf.getOption(PROP_EXTERNAL_EXTRA_JARS._1)

  def externalCommunicationCompression: String = sparkConf.get(PROP_EXTERNAL_COMMUNICATION_COMPRESSION._1, PROP_EXTERNAL_COMMUNICATION_COMPRESSION._2)

  private[backend] def isBackendVersionCheckDisabled = sparkConf.getBoolean(PROP_EXTERNAL_DISABLE_VERSION_CHECK._1, PROP_EXTERNAL_DISABLE_VERSION_CHECK._2)

  /** Setters */

  /**
   * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
   * This method automatically sets external cluster mode
   *
   * @param host host representing the cluster
   * @param port port representing the cluster
   * @return H2O Configuration
   */
  def setH2OCluster(host: String, port: Int): H2OConf = {
    logWarning("The method 'setH2OCluster(host: String, port: Int)' also sets backend to external. " +
      "This side effect will be removed in the version 3.32.")
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
  }

  def setH2OCluster(hostPort: String): H2OConf = {
    logWarning("The method 'setH2OCluster(hostPort: String)' also sets backend to external. " +
      "This side effect will be removed in the version in 3.32.")
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, hostPort)
  }

  def setClusterSize(clusterSize: Int): H2OConf = set(PROP_EXTERNAL_CLUSTER_SIZE._1, clusterSize.toString)

  def setClusterStartTimeout(clusterStartTimeout: Int): H2OConf = set(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, clusterStartTimeout.toString)

  def setClusterInfoFile(path: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_INFO_FILE._1, path)

  def setMapperXmx(mem: String): H2OConf = set(PROP_EXTERNAL_H2O_MEMORY._1, mem)

  def setHDFSOutputDir(dir: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)

  def useAutoClusterStart(): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "auto")
  }

  def useManualClusterStart(): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "manual")
  }

  def setH2ODriverPath(path: String): H2OConf = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1, path)
  }

  def setYARNQueue(queueName: String): H2OConf = set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)

  def setKillOnUnhealthyClusterEnabled(): H2OConf = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, value = true)

  def setKillOnUnhealthyClusterDisabled(): H2OConf = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, value = false)

  def setKerberosPrincipal(principal: String): H2OConf = set(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1, principal)

  def setKerberosKeytab(path: String): H2OConf = set(PROP_EXTERNAL_KERBEROS_KEYTAB._1, path)

  def setRunAsUser(user: String): H2OConf = set(PROP_EXTERNAL_RUN_AS_USER._1, user)

  def setExternalH2ODriverIf(iface: String): H2OConf = set(PROP_EXTERNAL_DRIVER_IF._1, iface)

  def setExternalH2ODriverPort(port: Int): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT._1, port.toString)

  def setExternalH2ODriverPortRange(portRange: String): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT_RANGE._1, portRange)

  def setExternalExtraMemoryPercent(memoryPercent: Int): H2OConf = set(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, memoryPercent.toString)

  def setExternalBackendStopTimeout(timeout: Int): H2OConf = set(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, timeout.toString)

  def setExternalHadoopExecutable(executable: String): H2OConf = set(PROP_EXTERNAL_HADOOP_EXECUTABLE._1, executable)

  def setExternalExtraJars(commaSeparatedPaths: String): H2OConf = set(PROP_EXTERNAL_EXTRA_JARS._1, commaSeparatedPaths)

  def setExternalExtraJars(paths: java.util.ArrayList[String]): H2OConf = setExternalExtraJars(paths.asScala)

  def setExternalExtraJars(paths: Seq[String]): H2OConf = setExternalExtraJars(paths.mkString(","))

  def setExternalCommunicationCompression(compressionType: String): H2OConf = {
    set(PROP_EXTERNAL_COMMUNICATION_COMPRESSION._1, compressionType)
  }

  def externalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  cluster start mode   : $clusterStartMode
       |  cloudName            : ${cloudName.getOrElse("Not set yet")}
       |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
       |  clientBasePort       : $clientBasePort
       |  h2oClientLog         : $h2oClientLogLevel
       |  nthreads             : $nthreads""".stripMargin
}

object ExternalBackendConf {

  val EXTERNAL_BACKEND_AUTO_MODE: String = "auto"
  val EXTERNAL_BACKEND_MANUAL_MODE: String = "manual"

  val PROP_EXTERNAL_DRIVER_IF: (String, None.type) = ("spark.ext.h2o.external.driver.if", None)

  val PROP_EXTERNAL_DRIVER_PORT: (String, None.type) = ("spark.ext.h2o.external.driver.port", None)

  val PROP_EXTERNAL_DRIVER_PORT_RANGE: (String, None.type) = ("spark.ext.h2o.external.driver.port.range", None)

  val PROP_EXTERNAL_EXTRA_MEMORY_PERCENT: (String, Int) = ("spark.ext.h2o.external.extra.memory.percent", 10)

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE: (String, None.type) = ("spark.ext.h2o.cloud.representative", None)

  /** Number of nodes to wait for when connecting to external H2O cluster in manual mode. In auto mode, number
   * of nodes to be started. This option does not have any effect in manual cluster start in case
   * spark.ext.h2o.rest.api.based.client is set to true */
  val PROP_EXTERNAL_CLUSTER_SIZE: (String, None.type) = ("spark.ext.h2o.external.cluster.size", None)

  /** Timeout in seconds for starting h2o external cluster */
  val PROP_EXTERNAL_CLUSTER_START_TIMEOUT: (String, Int) = ("spark.ext.h2o.cluster.start.timeout", 120)

  /** Path to a file used as cluster notification file */
  val PROP_EXTERNAL_CLUSTER_INFO_FILE: (String, None.type) = ("spark.ext.h2o.cluster.info.name", None)

  /** Number of memory assigned to each external h2o node when starting in auto mode */
  val PROP_EXTERNAL_H2O_MEMORY: (String, String) = ("spark.ext.h2o.hadoop.memory", "6g")

  /** HDFS dir for external h2o nodes when starting in auto mode */
  val PROP_EXTERNAL_CLUSTER_HDFS_DIR: (String, None.type) = ("spark.ext.h2o.external.hdfs.dir", None)

  /**
   * If this option is set to "auto" then h2o external cluster will be automatically started using the provided
   * h2o driver on yarn, otherwise it is expected that the cluster will be started by the user
   */
  val PROP_EXTERNAL_CLUSTER_START_MODE: (String, String) = ("spark.ext.h2o.external.start.mode", EXTERNAL_BACKEND_MANUAL_MODE)

  /** Path to h2o driver */
  val PROP_EXTERNAL_CLUSTER_DRIVER_PATH: (String, None.type) = ("spark.ext.h2o.external.h2o.driver", None)

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE: (String, None.type) = ("spark.ext.h2o.external.yarn.queue", None)

  /**
   * If true, the client will try to kill the cluster and then itself in case some nodes in the cluster report unhealthy status
   */
  val PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY: (String, Boolean) = ("spark.ext.h2o.external.kill.on.unhealthy", true)

  /**
   * Kerberos principal
   */
  val PROP_EXTERNAL_KERBEROS_PRINCIPAL: (String, None.type) = ("spark.ext.h2o.external.kerberos.principal", None)

  /**
   * Path to Kerberos key tab
   */
  val PROP_EXTERNAL_KERBEROS_KEYTAB: (String, None.type) = ("spark.ext.h2o.external.kerberos.keytab", None)

  /**
   * Impersonated Hadoop user
   */
  val PROP_EXTERNAL_RUN_AS_USER: (String, None.type) = ("spark.ext.h2o.external.run.as.user", None)

  /**
   * Timeout for confirmation from worker nodes when stopping the external backend. It is also
   * possible to pass -1 to ensure the indefinite timeout. The unit is milliseconds.
   */
  val PROP_EXTERNAL_BACKEND_STOP_TIMEOUT: (String, Int) = ("spark.ext.h2o.external.backend.stop.timeout", 10000)

  /** Disable version check of external H2O backend */
  val PROP_EXTERNAL_DISABLE_VERSION_CHECK: (String, Boolean) = ("spark.ext.h2o.external.disable.version.check", false)

  /** Hadoop executable used to start external H2O cluster on Hadoop */
  val PROP_EXTERNAL_HADOOP_EXECUTABLE: (String, String) = ("spark.ext.h2o.external.hadoop.executable", "hadoop")

  /** Comma-separated paths to jar files that will be placed onto classpath of each H2O node. */
  val PROP_EXTERNAL_EXTRA_JARS: (String, None.type) = ("spark.ext.h2o.external.extra.jars", None)

  /** The type of compression used for data transfer between Spark and H2O nodes */
  val PROP_EXTERNAL_COMMUNICATION_COMPRESSION: (String, String) = {
    ("spark.ext.h2o.external.communication.compression", Compression.defaultCompression)
  }

  /** ID of external H2O backend started on YARN application */
  val PROP_EXTERNAL_CLUSTER_YARN_APP_ID: (String, None.type) = ("spark.ext.h2o.external.yarn.app.id", None)
}
