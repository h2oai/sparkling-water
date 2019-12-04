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

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedBackendConf
import water.HeartBeatThread

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import ExternalBackendConf._

  /** Getters */

  def h2oCluster = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
  def h2oClusterHost = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(0))
  def h2oClusterPort = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(1).toInt)

  def clusterSize = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_SIZE._1)
  def clientConnectionTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._2)
  def externalWriteConfirmationTimeout = sparkConf.getInt(PROP_EXTERNAL_WRITE_TIMEOUT._1, PROP_EXTERNAL_WRITE_TIMEOUT._2)
  def clusterStartTimeout = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, PROP_EXTERNAL_CLUSTER_START_TIMEOUT._2)
  def clusterInfoFile = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_INFO_FILE._1)
  def mapperXmx = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)
  def HDFSOutputDir = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)

  def isAutoClusterStartUsed = clusterStartMode == EXTERNAL_BACKEND_AUTO_MODE
  def isManualClusterStartUsed = clusterStartMode == EXTERNAL_BACKEND_MANUAL_MODE

  def clusterStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)
  def h2oDriverPath = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)
  def YARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)
  def h2oDriverIf = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_IF._1)
  def healthCheckInterval = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._2)
  def isKillOnUnhealthyClusterEnabled = sparkConf.getBoolean(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._2)
  def killOnUnhealthyClusterInterval = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._2)
  def kerberosPrincipal = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1)
  def kerberosKeytab = sparkConf.getOption(PROP_EXTERNAL_KERBEROS_KEYTAB._1)
  def runAsUser = sparkConf.getOption(PROP_EXTERNAL_RUN_AS_USER._1)
  def externalH2ODriverIf = sparkConf.getOption(PROP_EXTERNAL_DRIVER_IF._1)
  def externalH2ODriverPort = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT._1)
  def externalH2ODriverPortRange = sparkConf.getOption(PROP_EXTERNAL_DRIVER_PORT_RANGE._1)
  def externalExtraMemoryPercent = sparkConf.getInt(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._2)
  def externalCommunicationBlockSizeAsBytes: Long = sparkConf.getSizeAsBytes(
    PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE._1,
    PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE._2)
  def externalCommunicationBlockSize: String = sparkConf.get(PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE._1, PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE._2)
  def externalBackendStopTimeout: Int = sparkConf.getInt(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._2)
  private[backends] def isBackendVersionCheckDisabled() = sparkConf.getBoolean(PROP_EXTERNAL_DISABLE_VERSION_CHECK._1, PROP_EXTERNAL_DISABLE_VERSION_CHECK._2)

  /** Setters */

  /**
    * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
    * This method automatically sets external cluster mode
    *
    * @param host host representing the cluster
    * @param port port representing the cluster
    * @return H2O Configuration
    */
  def setH2OCluster(host: String, port: Int) = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
  }

  def setH2OCluster(hostPort: String) = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, hostPort)
  }

  def setClusterSize(clusterSize: Int) = set(PROP_EXTERNAL_CLUSTER_SIZE._1, clusterSize.toString)

  def setClientConnectionTimeout(timeout: Int) = set(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, timeout.toString)
  def setExternalWriteConfirmationTimeout(timeout: Int) = set(PROP_EXTERNAL_WRITE_TIMEOUT._1, timeout.toString)
  def setClusterStartTimeout(clusterStartTimeout: Int) = set(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, clusterStartTimeout.toString)
  def setClusterConfigFile(path: String) = set(PROP_EXTERNAL_CLUSTER_INFO_FILE._1, path)
  def setMapperXmx(mem: String) = set(PROP_EXTERNAL_H2O_MEMORY._1, mem)
  def setHDFSOutputDir(dir: String) = set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)

  def useAutoClusterStart() = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "auto")
  }

  def useManualClusterStart()  = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "manual")
  }

  def setH2ODriverPath(path: String) = {
    setExternalClusterMode()
    set(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1, path)
  }

  def setYARNQueue(queueName: String) = set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)

  def setH2ODriverIf(ip: String) = set(PROP_EXTERNAL_CLUSTER_DRIVER_IF._1, ip)

  def setHealthCheckInterval(interval: Int) = set(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, interval.toString)

  def setKillOnUnhealthyClusterEnabled() = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, true)
  def setKillOnUnhealthyClusterDisabled() = set(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, false)

  def setKillOnUnhealthyClusterInterval(interval: Int) = set(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, interval.toString)

  def setKerberosPrincipal(principal: String) = set(PROP_EXTERNAL_KERBEROS_PRINCIPAL._1, principal)
  def setKerberosKeytab(path: String) = set(PROP_EXTERNAL_KERBEROS_KEYTAB._1, path)
  def setRunAsUser(user: String) = set(PROP_EXTERNAL_RUN_AS_USER._1, user)

  def setExternalH2ODriverIf(iface: String): H2OConf = set(PROP_EXTERNAL_DRIVER_IF._1, iface)
  def setExternalH2ODriverPort(port: Int): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT._1, port.toString)
  def setExternalH2ODriverPortRange(portRange: String): H2OConf = set(PROP_EXTERNAL_DRIVER_PORT_RANGE._1, portRange)
  def setExternalExtraMemoryPercent(memoryPercent: Int): H2OConf = set(PROP_EXTERNAL_EXTRA_MEMORY_PERCENT._1, memoryPercent.toString)
  def setExternalCommunicationBlockSize(blockSize: String): H2OConf = set(PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE._1, blockSize)
  def setExternalBackendStopTimeout(timeout: Int): H2OConf = set(PROP_EXTERNAL_BACKEND_STOP_TIMEOUT._1, timeout.toString)

  def externalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  cluster start mode   : ${clusterStartMode}
        |  cloudName            : ${cloudName.getOrElse("Not set yet")}
        |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
        |  clientBasePort       : ${clientBasePort}
        |  h2oClientLog         : ${h2oClientLogLevel}
        |  nthreads             : ${nthreads}""".stripMargin
}

object ExternalBackendConf {

  val EXTERNAL_BACKEND_AUTO_MODE = "auto"
  val EXTERNAL_BACKEND_MANUAL_MODE = "manual"

  val PROP_EXTERNAL_DRIVER_IF = ("spark.ext.h2o.external.driver.if", None)

  val PROP_EXTERNAL_DRIVER_PORT = ("spark.ext.h2o.external.driver.port", None)

  val PROP_EXTERNAL_DRIVER_PORT_RANGE = ("spark.ext.h2o.external.driver.port.range", None)

  val PROP_EXTERNAL_EXTRA_MEMORY_PERCENT = ("spark.ext.h2o.external.extra.memory.percent", 10)

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative", None)

  /** Number of nodes to wait for when connecting to external H2O cluster in manual mode. In auto mode, number
    * of nodes to be started. This option does not have any effect in manual cluster start in case
    * spark.ext.h2o.rest.api.based.client is set to true */
  val PROP_EXTERNAL_CLUSTER_SIZE = ("spark.ext.h2o.external.cluster.size", None)

  /** Timeout in milliseconds for watchdog client connection. If client is not connected
    * to the external cluster in the given time, the cluster is killed */
  val PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT = ("spark.ext.h2o.cluster.client.connect.timeout", 120000 + 60000)

  /** Timeout in seconds for confirmation of write operation ( spark frame => h2o frame) on external cluster. */
  val PROP_EXTERNAL_WRITE_TIMEOUT = ("spark.ext.h2o.external.write.confirmation.timeout", 60)

  /** Timeout in seconds for starting h2o external cluster */
  val PROP_EXTERNAL_CLUSTER_START_TIMEOUT = ("spark.ext.h2o.cluster.start.timeout", 120)

  /** Path to a file used as cluster notification file */
  val PROP_EXTERNAL_CLUSTER_INFO_FILE = ("spark.ext.h2o.cluster.info.name", None)

  /** Number of memory assigned to each external h2o node when starting in auto mode */
  val PROP_EXTERNAL_H2O_MEMORY = ("spark.ext.h2o.hadoop.memory", "6g")

  /** HDFS dir for external h2o nodes when starting in auto mode */
  val PROP_EXTERNAL_CLUSTER_HDFS_DIR = ("spark.ext.h2o.external.hdfs.dir", None)

  /**
    * If this option is set to "auto" then h2o external cluster will be automatically started using the provided
    * h2o driver on yarn, otherwise it is expected that the cluster will be started by the user
    */
  val PROP_EXTERNAL_CLUSTER_START_MODE = ("spark.ext.h2o.external.start.mode", EXTERNAL_BACKEND_MANUAL_MODE)

  /** Path to h2o driver */
  val PROP_EXTERNAL_CLUSTER_DRIVER_PATH = ("spark.ext.h2o.external.h2o.driver", None)

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE = ("spark.ext.h2o.external.yarn.queue", None)

  /** Driver IP address in case of auto mode in external cluster backend */
  val PROP_EXTERNAL_CLUSTER_DRIVER_IF = ("spark.ext.h2o.external.driver.if", None)

  /** Health check interval for external H2O nodes
    */
  val PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL = ("spark.ext.h2o.external.health.check.interval", HeartBeatThread.TIMEOUT)

  /**
    * If true, the client will try to kill the cluster and then itself in case some nodes in the cluster report unhealthy status
    */
  val PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY = ("spark.ext.h2o.external.kill.on.unhealthy", true)

  /**
    * How often check the healthy status for the decision whether to kill the cloud or not.
    */
  val PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY_INTERVAL = ("spark.ext.h2o.external.kill.on.unhealthy.interval", HeartBeatThread.TIMEOUT * 3)

  /**
    * Kerberos principal
    */
  val PROP_EXTERNAL_KERBEROS_PRINCIPAL = ("spark.ext.h2o.external.kerberos.principal", None)

  /**
    * Path to Kerberos key tab
    */
  val PROP_EXTERNAL_KERBEROS_KEYTAB = ("spark.ext.h2o.external.kerberos.keytab", None)

  /**
    * Impersonated Hadoop user
    */
  val PROP_EXTERNAL_RUN_AS_USER = ("spark.ext.h2o.external.run.as.user", None)

  /**
    * The size of blocks representing data traffic from Spark nodes to H2O-3 nodes.
    */
  val PROP_EXTERNAL_COMMUNICATION_BLOCK_SIZE = ("spark.ext.h2o.external.communication.blockSize", "1m")

  /**
    * How long we wait before for proper stopping of automatic mode of external backend before we give
    * up on confirmation from worker nodes. It is also possible to pass -1 to ensure we wait indefinitely.
    * The unit is milliseconds.
    */
  val PROP_EXTERNAL_BACKEND_STOP_TIMEOUT = ("spark.ext.h2o.external.backend.stop.timeout", 10000)

  /** Disable version check of external H2O backend */
  val PROP_EXTERNAL_DISABLE_VERSION_CHECK = ("spark.ext.h2o.external.disable.version.check", false)
}
