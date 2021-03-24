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

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.H2OConf.{IntOption, OptionOption}
import ai.h2o.sparkling.backend.SharedBackendConf
import ai.h2o.sparkling.macros.DeprecatedMethod
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkEnv
import org.apache.spark.expose.Utils

/**
  * Internal backend configuration
  */
trait InternalBackendConf extends SharedBackendConf with InternalBackendConfExtensions {
  self: H2OConf =>

  import InternalBackendConf._

  /** Getters */
  def numH2OWorkers: Option[Int] = sparkConf.getOption(PROP_CLUSTER_SIZE._1).map(_.toInt)

  def drddMulFactor: Int = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)

  def numRddRetries: Int = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)

  def defaultCloudSize: Int = sparkConf.getInt(PROP_DEFAULT_CLUSTER_SIZE._1, PROP_DEFAULT_CLUSTER_SIZE._2)

  def subseqTries: Int = sparkConf.getInt(PROP_SUBSEQ_TRIES._1, PROP_SUBSEQ_TRIES._2)

  @DeprecatedMethod("icedDir", "3.34")
  def nodeIcedDir: Option[String] = sparkConf.getOption(SharedBackendConf.PROP_ICED_DIR._1)

  def hdfsConf: Option[String] = sparkConf.getOption(PROP_HDFS_CONF._1)

  def spreadRddRetriesTimeout: Int =
    sparkConf.getInt(PROP_SPREADRDD_RETRIES_TIMEOUT._1, PROP_SPREADRDD_RETRIES_TIMEOUT._2)

  /** Setters */
  def setNumH2OWorkers(numWorkers: Int): H2OConf = set(PROP_CLUSTER_SIZE._1, numWorkers.toString)

  def setDrddMulFactor(factor: Int): H2OConf = set(PROP_DUMMY_RDD_MUL_FACTOR._1, factor.toString)

  def setNumRddRetries(retries: Int): H2OConf = set(PROP_SPREADRDD_RETRIES._1, retries.toString)

  def setDefaultCloudSize(defaultClusterSize: Int): H2OConf =
    set(PROP_DEFAULT_CLUSTER_SIZE._1, defaultClusterSize.toString)

  def setSubseqTries(subseqTriesNum: Int): H2OConf = set(PROP_SUBSEQ_TRIES._1, subseqTriesNum.toString)

  @DeprecatedMethod("setIcedDir", "3.34")
  def setNodeIcedDir(dir: String): H2OConf = set(SharedBackendConf.PROP_ICED_DIR._1, dir)

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

  def setSpreadRddRetriesTimeout(timeout: Int): H2OConf = set(PROP_SPREADRDD_RETRIES_TIMEOUT._1, timeout.toString)
}

object InternalBackendConf {

  val PROP_CLUSTER_SIZE: OptionOption = (
    "spark.ext.h2o.cluster.size",
    None,
    "setNumH2OWorkers(Integer)",
    """Expected number of workers of H2O cluster. Value None means automatic
      |detection of cluster size. This number must be equal to number of Spark executors""".stripMargin)

  val PROP_DUMMY_RDD_MUL_FACTOR: IntOption = (
    "spark.ext.h2o.dummy.rdd.mul.factor",
    10,
    "setDrddMulFactor(Integer)",
    """Multiplication factor for dummy RDD  generation. Size of dummy RDD is
      |``spark.ext.h2o.cluster.size`` multiplied by this option.""".stripMargin)

  val PROP_SPREADRDD_RETRIES: IntOption = (
    "spark.ext.h2o.spreadrdd.retries",
    10,
    "setNumRddRetries(Integer)",
    "Number of retries for creation of an RDD spread across all existing Spark executors")

  val PROP_DEFAULT_CLUSTER_SIZE: IntOption = (
    "spark.ext.h2o.default.cluster.size",
    20,
    "setDefaultCloudSize(Integer)",
    "Starting size of cluster in case that size is not explicitly configured.")

  val PROP_SUBSEQ_TRIES: IntOption = (
    "spark.ext.h2o.subseq.tries",
    5,
    "setSubseqTries(Integer)",
    """Subsequent successful tries to figure out size of Spark cluster, which are
      |producing the same number of nodes.""".stripMargin)

  val PROP_HDFS_CONF: OptionOption = (
    "spark.ext.h2o.hdfs_conf",
    None,
    "setHdfsConf(String)",
    """Either a string with the Path to a file with Hadoop HDFS configuration or the
      |hadoop.conf.Configuration object in the org.apache package. Useful for HDFS credentials
      |settings and other HDFS-related configurations. Default value None means
      |use `sc.hadoopConfig`.""".stripMargin)

  val PROP_SPREADRDD_RETRIES_TIMEOUT: IntOption = (
    "spark.ext.h2o.spreadrdd.retries.timeout",
    0,
    "setSpreadRddRetriesTimeout(Int)",
    """Specifies how long the discovering of Spark executors should last. This
      |option has precedence over other options influencing the discovery
      |mechanism. That means that as long as the timeout hasn't expired, we keep
      |trying to discover new executors. This option might be useful in environments
      |where Spark executors might join the cloud with some delays.""".stripMargin)
}
