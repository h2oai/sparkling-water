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

package ai.h2o.sparkling.backend.internal

import java.io.{File, FileWriter}

import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkEnv
import org.apache.spark.expose.Utils
import org.apache.spark.h2o.H2OConf

/**
 * Internal backend configuration
 */
trait InternalBackendConf extends SharedBackendConf {
  self: H2OConf =>

  import InternalBackendConf._

  /** Getters */
  def numH2OWorkers: Option[Int] = sparkConf.getOption(PROP_CLUSTER_SIZE._1).map(_.toInt)

  def drddMulFactor: Int = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)

  def numRddRetries: Int = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)

  def defaultCloudSize: Int = sparkConf.getInt(PROP_DEFAULT_CLUSTER_SIZE._1, PROP_DEFAULT_CLUSTER_SIZE._2)

  def subseqTries: Int = sparkConf.getInt(PROP_SUBSEQ_TRIES._1, PROP_SUBSEQ_TRIES._2)

  def nodeIcedDir: Option[String] = sparkConf.getOption(PROP_NODE_ICED_DIR._1)

  def hdfsConf: Option[String] = sparkConf.getOption(PROP_HDFS_CONF._1)

  /** Setters */
  def setNumH2OWorkers(numWorkers: Int): H2OConf = set(PROP_CLUSTER_SIZE._1, numWorkers.toString)

  def setDrddMulFactor(factor: Int): H2OConf = set(PROP_DUMMY_RDD_MUL_FACTOR._1, factor.toString)

  def setNumRddRetries(retries: Int): H2OConf = set(PROP_SPREADRDD_RETRIES._1, retries.toString)

  def setDefaultCloudSize(defaultClusterSize: Int): H2OConf = set(PROP_DEFAULT_CLUSTER_SIZE._1, defaultClusterSize.toString)

  def setSubseqTries(subseqTriesNum: Int): H2OConf = set(PROP_SUBSEQ_TRIES._1, subseqTriesNum.toString)

  def setNodeIcedDir(dir: String): H2OConf = set(PROP_NODE_ICED_DIR._1, dir)

  def setHdfsConf(path: String): H2OConf = set(PROP_HDFS_CONF._1, path)

  def setHdfsConf(conf: Configuration): H2OConf = {
    val sparkTmpDir = new File(Utils.getLocalDir(SparkEnv.get.conf))
    val hdfsConfigTempFile = File.createTempFile("hdfs_conf", ".xml", sparkTmpDir)
    hdfsConfigTempFile.deleteOnExit()
    withResource(new FileWriter(hdfsConfigTempFile)) { fileWriter =>
      conf.writeXml(fileWriter)
    }
    set(PROP_HDFS_CONF._1, hdfsConfigTempFile.getAbsolutePath)
  }

  def internalConfString: String =
    s"""Sparkling Water configuration:
       |  backend cluster mode : $backendClusterMode
       |  workers              : $numH2OWorkers
       |  cloudName            : ${cloudName.getOrElse("Not set yet, it will be set automatically before starting H2OContext.")}
       |  clientBasePort       : $clientBasePort
       |  nodeBasePort         : $nodeBasePort
       |  cloudTimeout         : $cloudTimeout
       |  h2oNodeLog           : $h2oNodeLogLevel
       |  h2oClientLog         : $h2oClientLogLevel
       |  nthreads             : $nthreads
       |  drddMulFactor        : $drddMulFactor""".stripMargin

  private[backend] override def getFileProperties: Seq[(String, _)] = super.getFileProperties :+ PROP_HDFS_CONF
}

object InternalBackendConf {
  /** Configuration property - expected number of workers of H2O cloud.
   * Value None means automatic detection of cluster size.
   */
  val PROP_CLUSTER_SIZE: (String, None.type) = ("spark.ext.h2o.cluster.size", None)

  /** Configuration property - multiplication factor for dummy RDD generation.
   * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_DUMMY_RDD_MUL_FACTOR: (String, Int) = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  /** Configuration property - number of retries to create an RDD spread over all executors */
  val PROP_SPREADRDD_RETRIES: (String, Int) = ("spark.ext.h2o.spreadrdd.retries", 10)

  /** Starting size of cluster in case that size is not explicitly passed */
  val PROP_DEFAULT_CLUSTER_SIZE: (String, Int) = ("spark.ext.h2o.default.cluster.size", 20)

  /** Subsequent successful tries to figure out size of Spark cluster which are producing same number of nodes. */
  val PROP_SUBSEQ_TRIES: (String, Int) = ("spark.ext.h2o.subseq.tries", 5)

  /** Location of iced directory for Spark nodes */
  val PROP_NODE_ICED_DIR: (String, None.type) = ("spark.ext.h2o.node.iced.dir", None)

  /** Path to whole Hadoop configuration serialized into XML readable by org.hadoop.Configuration class */
  val PROP_HDFS_CONF: (String, None.type) = ("spark.ext.h2o.hdfs_conf", None)
}
