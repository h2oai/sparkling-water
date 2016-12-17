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

package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.backends.SharedBackendConf

/**
  * Internal backend configuration
  */
trait InternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import InternalBackendConf._
  def numH2OWorkers = sparkConf.getOption(PROP_CLUSTER_SIZE._1).map(_.toInt)
  def useFlatFile   = sparkConf.getBoolean(PROP_USE_FLATFILE._1, PROP_USE_FLATFILE._2)
  def nodeBasePort  = sparkConf.getInt(PROP_NODE_PORT_BASE._1, PROP_NODE_PORT_BASE._2)
  def cloudTimeout  = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)
  def drddMulFactor = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)
  def numRddRetries = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)
  def defaultCloudSize  = sparkConf.getInt(PROP_DEFAULT_CLUSTER_SIZE._1, PROP_DEFAULT_CLUSTER_SIZE._2)
  def h2oNodeLogLevel   = sparkConf.get(PROP_NODE_LOG_LEVEL._1, PROP_NODE_LOG_LEVEL._2)
  def h2oNodeLogDir   = sparkConf.get(PROP_NODE_LOG_DIR._1, PROP_NODE_LOG_DIR._2)
  def nodeNetworkMask   = sparkConf.getOption(PROP_NODE_NETWORK_MASK._1)
  def nodeIcedDir   = sparkConf.getOption(PROP_NODE_ICED_DIR._1)
  def subseqTries  = sparkConf.getInt(PROP_SUBSEQ_TRIES._1, PROP_SUBSEQ_TRIES._2)

  def internalConfString: String =
    s"""Sparkling Water configuration:
        |  backend cluster mode : ${backendClusterMode}
        |  workers              : ${numH2OWorkers}
        |  cloudName            : ${cloudName.getOrElse("Not set yet, it will be set automatically before starting H2OContext.")}
        |  flatfile             : ${useFlatFile}
        |  clientBasePort       : ${clientBasePort}
        |  nodeBasePort         : ${nodeBasePort}
        |  cloudTimeout         : ${cloudTimeout}
        |  h2oNodeLog           : ${h2oNodeLogLevel}
        |  h2oClientLog         : ${h2oClientLogLevel}
        |  nthreads             : ${nthreads}
        |  drddMulFactor        : $drddMulFactor""".stripMargin


  def setH2ONodeLogLevel(level: String): H2OConf = {
    sparkConf.set(PROP_NODE_LOG_LEVEL._1, level)
    self
  }
}

object InternalBackendConf {
  /** Configuration property - expected number of workers of H2O cloud.
    * Value None means automatic detection of cluster size.
    */
  val PROP_CLUSTER_SIZE = ("spark.ext.h2o.cluster.size", None)

  /** Configuration property - use flatfile for H2O cloud formation. */
  val PROP_USE_FLATFILE = ("spark.ext.h2o.flatfile", true)

  /** Configuration property - base port used for individual H2O nodes configuration. */
  val PROP_NODE_PORT_BASE = ( "spark.ext.h2o.node.port.base", 54321)

  /** Configuration property - timeout for cloud up. */
  val PROP_CLOUD_TIMEOUT = ("spark.ext.h2o.cloud.timeout", 60*1000)

  /** Configuration property - multiplication factor for dummy RDD generation.
    * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_DUMMY_RDD_MUL_FACTOR = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  /** Configuration property - number of retries to create an RDD spread over all executors */
  val PROP_SPREADRDD_RETRIES = ("spark.ext.h2o.spreadrdd.retries", 10)

  /** Starting size of cluster in case that size is not explicitly passed */
  val PROP_DEFAULT_CLUSTER_SIZE = ("spark.ext.h2o.default.cluster.size", 20)

  /** H2O internal log level for launched remote nodes. */
  val PROP_NODE_LOG_LEVEL = ("spark.ext.h2o.node.log.level", "INFO")

  /** Location of log directory for remote nodes. */
  val PROP_NODE_LOG_DIR = ("spark.ext.h2o.node.log.dir", null.asInstanceOf[String])

  /** Subnet selector for H2O nodes running inside executors - if the mask is specified then Spark network setup is not discussed. */
  val PROP_NODE_NETWORK_MASK = ("spark.ext.h2o.node.network.mask", null.asInstanceOf[String])

  /** Location of iced directory for Spark nodes */
  val PROP_NODE_ICED_DIR = ("spark.ext.h2o.node.iced.dir", null.asInstanceOf[String])

  /** Subsequent successful tries to figure out size of Spark cluster which are producing same number of nodes. */
  val PROP_SUBSEQ_TRIES = ("spark.ext.h2o.subseq.tries", 5)
}
