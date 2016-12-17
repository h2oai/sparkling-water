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

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import ExternalBackendConf._

  def h2oCluster = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
  def YARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)
  def h2oDriverPath = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1)
  def numOfExternalH2ONodes = sparkConf.getOption(PROP_EXTERNAL_H2O_NODES._1)
  def HDFSOutputDir = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)
  def mapperXmx = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)
  def clusterInfoFile = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_INFO_FILE._1)
  def clusterStartMode = sparkConf.get(PROP_EXTERNAL_CLUSTER_START_MODE._1, PROP_EXTERNAL_CLUSTER_START_MODE._2)
  def isAutoClusterStartUsed = clusterStartMode == "auto"
  def isManualClusterStartUsed = !isAutoClusterStartUsed

  /**
    * Sets node and port representing H2O Cluster to which should H2O connect when started in external mode.
    * This method automatically sets external cluster mode
    *
    * @param host host representing the cluster
    * @param port port representing the cluster
    * @return H2O Configuration
    */
  def setH2OCluster(host: String, port: Int): H2OConf = {
    setExternalClusterMode()
    sparkConf.set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, host + ":" + port)
    self
  }

  def setH2OCluster(hostPort: String): H2OConf = {
    setExternalClusterMode()
    sparkConf.set(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1, hostPort)
    self
  }

  def h2oClusterHost = {
    sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(0))
  }

  def h2oClusterPort = {
    sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1).map(_.split(":")(1))
  }


  def setYARNQueue(queueName: String) = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)
    self
  }

  def setH2ODriverPath(path: String): H2OConf = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_DRIVER_PATH._1, path)
    self
  }

  def setNumOfExternalH2ONodes(numOfExternalH2ONodes: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_NODES._1, numOfExternalH2ONodes.toString)
    self
  }

  def setHDFSOutputDir(dir: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)
    self
  }


  def setMapperXmx(mem: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_MEMORY._1, mem)
    self
  }

  def setClusterConfigFile(path: String) = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_INFO_FILE._1, path)
    self
  }

  def useAutoClusterStart() = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "auto")
    self
  }

  def useManualClusterStart() = {
    setExternalClusterMode()
    logWarning("Using external cluster mode!")
    sparkConf.set(PROP_EXTERNAL_CLUSTER_START_MODE._1, "manual")
    self
  }

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

  val PROP_EXTERNAL_CLUSTER_INFO_FILE = ("spark.ext.h2o.cluster.info.name", null.asInstanceOf[String])

  val PROP_EXTERNAL_H2O_MEMORY = ("spark.ext.h2o.hadoop.memory", "6g")

  val PROP_EXTERNAL_CLUSTER_HDFS_DIR = ("spark.ext.h2o.external.hdfs.dir", null.asInstanceOf[String])

  /**
    * If this option is set to "auto" then h2o external cluster will be automatically started using the provided
    * h2o driver on yarn, otherwise it is expected that the cluster will be started by the user
    */
  val PROP_EXTERNAL_CLUSTER_START_MODE = ("spark.ext.h2o.external.start.mode", "manual")

  /**
    * Path to h2o driver
    */
  val PROP_EXTERNAL_CLUSTER_DRIVER_PATH = ("spark.ext.h2o.external.h2o.driver", null.asInstanceOf[String])

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE = ("spark.ext.h2o.external.yarn.queue", null.asInstanceOf[String])

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative", null.asInstanceOf[String])

  /** Number of nodes to wait for when connecting to external H2O cluster */
  val PROP_EXTERNAL_H2O_NODES = ("spark.ext.h2o.external.cluster.num.h2o.nodes", null.asInstanceOf[String])

}
