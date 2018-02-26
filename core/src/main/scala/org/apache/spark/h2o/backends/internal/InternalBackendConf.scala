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

  /** Getters */

  def useFlatFile = sparkConf.getBoolean(PROP_USE_FLATFILE._1, PROP_USE_FLATFILE._2)
  def numH2OWorkers = sparkConf.getOption(PROP_CLUSTER_SIZE._1).map(_.toInt)
  def drddMulFactor = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)
  def numRddRetries = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)
  def defaultCloudSize = sparkConf.getInt(PROP_DEFAULT_CLUSTER_SIZE._1, PROP_DEFAULT_CLUSTER_SIZE._2)
  def subseqTries = sparkConf.getInt(PROP_SUBSEQ_TRIES._1, PROP_SUBSEQ_TRIES._2)

  def nodeBasePort = sparkConf.getInt(PROP_NODE_PORT_BASE._1, PROP_NODE_PORT_BASE._2)
  def nodeIcedDir = sparkConf.getOption(PROP_NODE_ICED_DIR._1)

  def isInternalSecureConnectionsEnabled = sparkConf.getBoolean(PROP_INTERNAL_SECURE_CONNECTIONS._1,
                                                                PROP_INTERNAL_SECURE_CONNECTIONS._2)

  /** Setters */

  def setFlatFileEnabled() = set(PROP_USE_FLATFILE._1, true)
  def setFlatFileDisabled() = set(PROP_USE_FLATFILE._1, false)

  def setNumH2OWorkers(numWorkers: Int) = set(PROP_CLUSTER_SIZE._1, numWorkers.toString)
  def setDrddMulFactor(factor: Int) = set(PROP_DUMMY_RDD_MUL_FACTOR._1, factor.toString)
  def setNumRddRetries(retries: Int) = set(PROP_SPREADRDD_RETRIES._1, retries.toString)
  def setDefaultCloudSize(defaultClusterSize: Int) = set(PROP_DEFAULT_CLUSTER_SIZE._1, defaultClusterSize.toString)
  def setSubseqTries(subseqTriesNum: Int) = set(PROP_SUBSEQ_TRIES._1, subseqTriesNum.toString)

  def setNodeBasePort(port: Int) = set(PROP_NODE_PORT_BASE._1, port.toString)
  def setNodeIcedDir(dir: String) = set(PROP_NODE_ICED_DIR._1, dir)

  def setInternalSecureConnectionsEnabled() = set(PROP_INTERNAL_SECURE_CONNECTIONS._1, true)
  def setInternalSecureConnectionsDisabled() = set(PROP_INTERNAL_SECURE_CONNECTIONS._1, false)


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

}

object InternalBackendConf {

  /** Configuration property - use flatfile for H2O cloud formation. */
  val PROP_USE_FLATFILE = ("spark.ext.h2o.flatfile", true)

  /** Configuration property - expected number of workers of H2O cloud.
    * Value None means automatic detection of cluster size.
    */
  val PROP_CLUSTER_SIZE = ("spark.ext.h2o.cluster.size", None)

  /** Configuration property - multiplication factor for dummy RDD generation.
    * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_DUMMY_RDD_MUL_FACTOR = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  /** Configuration property - number of retries to create an RDD spread over all executors */
  val PROP_SPREADRDD_RETRIES = ("spark.ext.h2o.spreadrdd.retries", 10)

  /** Starting size of cluster in case that size is not explicitly passed */
  val PROP_DEFAULT_CLUSTER_SIZE = ("spark.ext.h2o.default.cluster.size", 20)

  /** Subsequent successful tries to figure out size of Spark cluster which are producing same number of nodes. */
  val PROP_SUBSEQ_TRIES = ("spark.ext.h2o.subseq.tries", 5)

  /** Configuration property - base port used for individual H2O nodes configuration. */
  val PROP_NODE_PORT_BASE = ( "spark.ext.h2o.node.port.base", 54321)

  /** Location of iced directory for Spark nodes */
  val PROP_NODE_ICED_DIR = ("spark.ext.h2o.node.iced.dir", None)

  /** Secure internal connections by automatically generated credentials */
  val PROP_INTERNAL_SECURE_CONNECTIONS = ("spark.ext.h2o.internal_secure_connections", false)

}
