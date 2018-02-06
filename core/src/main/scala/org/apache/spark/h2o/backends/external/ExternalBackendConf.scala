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

  def numOfExternalH2ONodes = sparkConf.getOption(PROP_EXTERNAL_H2O_NODES._1)
  def clientCheckRetryTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2)
  def clientConnectionTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._2)
  def externalReadConfirmationTimeout = sparkConf.getInt(PROP_EXTERNAL_READ_TIMEOUT._1, PROP_EXTERNAL_READ_TIMEOUT._2)
  def externalWriteConfirmationTimeout = sparkConf.getInt(PROP_EXTERNAL_WRITE_TIMEOUT._1, PROP_EXTERNAL_WRITE_TIMEOUT._2)
  def clusterStartTimeout = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_START_TIMEOUT._1, PROP_EXTERNAL_CLUSTER_START_TIMEOUT._2)
  def clusterInfoFile = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_INFO_FILE._1)
  def mapperXmx = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)
  def HDFSOutputDir = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)

  def isAutoClusterStartUsed = clusterStartMode == "auto"
  def isManualClusterStartUsed = !isAutoClusterStartUsed

  def clusterStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)
  def h2oDriverPath = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)
  def YARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)
  def h2oDriverIf = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_IF._1)
  def healthCheckInterval = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._2)
  def isKillOnUnhealthyClusterEnabled = sparkConf.getBoolean(PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._1, PROP_EXTERNAL_CLUSTER_KILL_ON_UNHEALTHY._2)
  def killOnUnhealthyClusterInterval = sparkConf.getInt(PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._1, PROP_EXTERNAL_CLUSTER_HEALTH_CHECK_INTERVAL._2)

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

  def setNumOfExternalH2ONodes(numOfExternalH2ONodes: Int) = set(PROP_EXTERNAL_H2O_NODES._1, numOfExternalH2ONodes.toString)
  def setClientCheckRetryTimeout(timeout: Int) = set(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, timeout.toString)
  def setClientConnectionTimeout(timeout: Int) = set(PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT._1, timeout.toString)
  def setExternalReadConfirmationTimeout(timeout: Int) = set(PROP_EXTERNAL_READ_TIMEOUT._1, timeout.toString)
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

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative", None)

  /** Number of nodes to wait for when connecting to external H2O cluster in manual mode. In auto mode, number
    * of nodes to be started. */
  val PROP_EXTERNAL_H2O_NODES = ("spark.ext.h2o.external.cluster.num.h2o.nodes", None)

  /** Timeout in milliseconds specifying how often the check for connected watchdog client is done */
  val PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT = ("spark.ext.h2o.cluster.client.retry.timeout", 60000)

  /** Timeout in milliseconds for watchdog client connection. If client is not connected
    * to the external cluster in the given time, the cluster is killed */
  val PROP_EXTERNAL_CLIENT_CONNECTION_TIMEOUT = ("spark.ext.h2o.cluster.client.connect.timeout", 120000 + 60000)

  /** Timeout in seconds for confirmation of read operation ( h2o frame => spark frame) on external cluster. */
  val PROP_EXTERNAL_READ_TIMEOUT = ("spark.ext.h2o.external.read.confirmation.timeout", 60)

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
  val PROP_EXTERNAL_CLUSTER_START_MODE = ("spark.ext.h2o.external.start.mode", "manual")

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
}
