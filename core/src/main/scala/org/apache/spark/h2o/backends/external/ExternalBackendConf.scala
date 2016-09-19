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
import org.apache.spark.h2o.backends.SharedH2OConf

/**
  * External backend configuration
  */
trait ExternalBackendConf extends SharedH2OConf {
  self: H2OConf =>

  import ExternalBackendConf._

  def h2oCluster = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
  def YARNQueue = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1)
  def driverPath = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_H2O_DRIVER._1)
  def numOfExternalH2ONodes = sparkConf.getOption(PROP_EXTERNAL_H2O_NODES._1)
  def hadoopLocation = sparkConf.get(PROP_EXTERNAL_HADOOP_LOCATION._1, PROP_EXTERNAL_HADOOP_LOCATION._2)
  def HDFSOutputDir = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1)
  def memoryForHadoopInstance = sparkConf.get(PROP_EXTERNAL_H2O_MEMORY._1, PROP_EXTERNAL_H2O_MEMORY._2)


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

  def h2oClusterHost = {
    val value = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
    if (value.isDefined) {
      Option(value.get.split(":")(0))
    } else {
      None
    }
  }

  def h2oClusterPort = {
    val value = sparkConf.getOption(PROP_EXTERNAL_CLUSTER_REPRESENTATIVE._1)
    if (value.isDefined) {
      Option(value.get.split(":")(1))
    } else {
      None
    }
  }


  def setYARNQueue(queueName: String) = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_YARN_QUEUE._1, queueName)
    self
  }

  def setDriverPath(path: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_H2O_DRIVER._1, path)
    self
  }

  def setNumOfExternalH2ONodes(numOfExternalH2ONodes: Int): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_NODES._1, numOfExternalH2ONodes.toString)
    self
  }

  def setHadoopLocation(hadoopLocation: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_HADOOP_LOCATION._1, hadoopLocation)
    self
  }

  def setHDFSOutputDir(dir: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_CLUSTER_HDFS_DIR._1, dir)
    self
  }


  def setMemoryForHadoopInstance(mem: String): H2OConf = {
    sparkConf.set(PROP_EXTERNAL_H2O_MEMORY._1, mem)
    self
  }


  def externalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  cloudName            : ${cloudName.getOrElse("Not set yet")}
        |  cloud representative : ${h2oCluster.getOrElse("Not set, using cloud name only")}
        |  clientBasePort       : ${clientBasePort}
        |  h2oClientLog         : ${h2oClientLogLevel}
        |  nthreads             : ${nthreads}""".stripMargin
}

object ExternalBackendConf {

  val PROP_EXTERNAL_H2O_MEMORY = ("spark.ext.h2o.hadoop.memory", "6g")
  
  val PROP_EXTERNAL_HADOOP_LOCATION = ("spark.ext.h2o.hadoop.location", "hadoop")

  val PROP_EXTERNAL_CLUSTER_HDFS_DIR = ("spark.ext.h2o.external.hdfs.dir", null.asInstanceOf[String])

  /**
    * If this option is set then h2o external cluster will be automatically started using the provided
    * h2o driver on yarn
    */
  val PROP_EXTERNAL_CLUSTER_H2O_DRIVER = ("spark.ext.h2o.external.h2o.driver", null.asInstanceOf[String])

  /** Yarn queue on which external cluster should be started */
  val PROP_EXTERNAL_CLUSTER_YARN_QUEUE = ("spark.ext.h2o.external.yarn.queue", null.asInstanceOf[String])

  /** ip:port of arbitrary h2o node to identify external h2o cluster */
  val PROP_EXTERNAL_CLUSTER_REPRESENTATIVE = ("spark.ext.h2o.cloud.representative", null.asInstanceOf[String])

  /** Number of nodes to wait for when connecting to external H2O cluster */
  val PROP_EXTERNAL_H2O_NODES = ("spark.ext.h2o.external.cluster.num.h2o.nodes", null.asInstanceOf[String])

}
