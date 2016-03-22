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

package org.apache.spark.h2o

import org.apache.spark.SparkConf

/**
 * Just simple configuration holder which is representing
 * properties passed from user to H2O App.
 */
trait H2OConf {

  /* Require Spar config */
  def sparkConf: SparkConf
  // Precondition
  require(sparkConf != null, "sparkConf was null")

  /* Initialize configuration */
  // Collect configuration properties
  import H2OConf._

  def numH2OWorkers = sparkConf.getOption(PROP_CLUSTER_SIZE._1).map(_.toInt)
  def useFlatFile   = sparkConf.getBoolean(PROP_USE_FLATFILE._1, PROP_USE_FLATFILE._2)
  def clientIp      = sparkConf.getOption(PROP_CLIENT_IP._1)
  def clientBasePort = sparkConf.getInt(PROP_CLIENT_PORT_BASE._1, PROP_CLIENT_PORT_BASE._2)
  def nodeBasePort   = sparkConf.getInt(PROP_NODE_PORT_BASE._1, PROP_NODE_PORT_BASE._2)
  def cloudTimeout  = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)
  def drddMulFactor = sparkConf.getInt(PROP_DUMMY_RDD_MUL_FACTOR._1, PROP_DUMMY_RDD_MUL_FACTOR._2)
  def numRddRetries = sparkConf.getInt(PROP_SPREADRDD_RETRIES._1, PROP_SPREADRDD_RETRIES._2)
  def cloudName     = sparkConf.get(PROP_CLOUD_NAME._1, PROP_CLOUD_NAME._2)
  def defaultCloudSize  = sparkConf.getInt(PROP_DEFAULT_CLUSTER_SIZE._1, PROP_DEFAULT_CLUSTER_SIZE._2)
  def h2oNodeLogLevel   = sparkConf.get(PROP_NODE_LOG_LEVEL._1, PROP_NODE_LOG_LEVEL._2)
  def h2oClientLogLevel = sparkConf.get(PROP_CLIENT_LOG_LEVEL._1, PROP_CLIENT_LOG_LEVEL._2)
  def h2oNodeLogDir   = sparkConf.get(PROP_NODE_LOG_DIR._1, PROP_NODE_LOG_DIR._2)
  def h2oClientLogDir = sparkConf.get(PROP_CLIENT_LOG_DIR._1, PROP_CLIENT_LOG_DIR._2)
  def networkMask   = sparkConf.getOption(PROP_NETWORK_MASK._1)
  def nthreads      = sparkConf.getInt(PROP_NTHREADS._1, PROP_NTHREADS._2)
  def disableGA     = sparkConf.getBoolean(PROP_DISABLE_GA._1, PROP_DISABLE_GA._2)
  def clientWebPort = sparkConf.getInt(PROP_CLIENT_WEB_PORT._1, PROP_CLIENT_WEB_PORT._2)
  def clientIcedDir = sparkConf.getOption(PROP_CLIENT_ICED_DIR._1)
  def nodeIcedDir   = sparkConf.getOption(PROP_NODE_ICED_DIR._1)

  def jks           = sparkConf.getOption(PROP_JKS._1)
  def jksPass       = sparkConf.getOption(PROP_JKS_PASS._1)
  def hashLogin     = sparkConf.getBoolean(PROP_HASH_LOGIN._1, PROP_HASH_LOGIN._2)
  def ldapLogin     = sparkConf.getBoolean(PROP_LDAP_LOGIN._1, PROP_LDAP_LOGIN._2)
  def loginConf     = sparkConf.getOption(PROP_LOGIN_CONF._1)
  def userName      = sparkConf.getOption(PROP_USER_NAME._1)

  def subseqTries  = sparkConf.getInt(PROP_SUBSEQ_TRIES._1, PROP_SUBSEQ_TRIES._2)
  def scalaIntDefaultNum = sparkConf.getInt(PROP_SCALA_INT_DEFAULT_NUM._1, PROP_SCALA_INT_DEFAULT_NUM._2)
  def h2oReplEnabled = sparkConf.getBoolean(PROP_REPL_ENABLED._1,PROP_REPL_ENABLED._2)
  /**
   * Produce arguments for H2O node based on this config.
   * @return array of H2O launcher command line arguments
   */
  def getH2ONodeArgs: Array[String] = (getH2OCommonOptions ++
                                       Seq("-log_level", h2oNodeLogLevel,
                                           "-baseport", nodeBasePort.toString)).toArray

  /**
   * Get arguments for H2O client.
   * @return array of H2O client arguments.
   */

  def getH2OClientArgs: Array[String] = (
    getH2OCommonOptions
      ++ Seq("-quiet")
      ++ (if (hashLogin) Seq("-hash_login") else Nil)
      ++ (if (ldapLogin) Seq("-ldap_login") else Nil)
      ++ Seq("-log_level", h2oClientLogLevel)
      ++ Seq("-log_dir", h2oClientLogDir)
      ++ Seq("-baseport", clientBasePort.toString)
      ++ Seq(
        ("-ice_root", clientIcedDir.orNull),
        ("-port", if (clientWebPort > 0) clientWebPort else null),
        ("-jks", jks.orNull),
        ("-jks_pass", jksPass.orNull),
        ("-login_conf", loginConf.orNull),
        ("-user_name", userName.orNull)
      ).filter(_._2 != null).flatMap(x => Seq(x._1, x._2.toString))
    ).toArray

  private def getH2OCommonOptions:Seq[String] =
    // Option in form key=value
    Seq(
      ("-name", cloudName),
      ("-nthreads", if (nthreads > 0) nthreads else null),
      ("-network", networkMask.orNull))
      .filter(x => x._2 != null)
      .flatMap(x => Seq(x._1, x._2.toString)) ++ // Append single boolean options
      Seq(("-ga_opt_out", disableGA))
        .filter(_._2).map(x => x._1)

  override def toString: String =
    s"""Sparkling Water configuration:
         |  workers        : $numH2OWorkers
         |  cloudName      : $cloudName
         |  flatfile       : $useFlatFile
         |  clientBasePort : $clientBasePort
         |  nodeBasePort   : $nodeBasePort
         |  cloudTimeout   : $cloudTimeout
         |  h2oNodeLog     : $h2oNodeLogLevel
         |  h2oClientLog   : $h2oClientLogLevel
         |  nthreads       : $nthreads
         |  drddMulFactor  : $drddMulFactor""".stripMargin
}

object H2OConf {
  /* Configuration properties */

  /** Configuration property - use flatfile for H2O cloud formation. */
  val PROP_USE_FLATFILE = ("spark.ext.h2o.flatfile", true)
  /** Configuration property - expected number of workers of H2O cloud.
    * Value None means automatic detection of cluster size.
    */
  val PROP_CLUSTER_SIZE = ("spark.ext.h2o.cluster.size", None)
  /** Configuration property - timeout for cloud up. */
  val PROP_CLOUD_TIMEOUT = ("spark.ext.h2o.cloud.timeout", 60*1000)
  /** Configuration property - number of retries to create an RDD spreat over all executors */
  val PROP_SPREADRDD_RETRIES = ("spark.ext.h2o.spreadrdd.retries", 10)
  /** Configuration property - name of H2O cloud */
  val PROP_CLOUD_NAME = ("spark.ext.h2o.cloud.name", "sparkling-water-")
  /** Starting size of cluster in case that size is not explicitelly passed */
  val PROP_DEFAULT_CLUSTER_SIZE = ("spark.ext.h2o.default.cluster.size", 20)
  /* H2O internal log level for launched remote nodes. */
  val PROP_NODE_LOG_LEVEL = ("spark.ext.h2o.node.log.level", "INFO")
  /** H2O log leve for client running in Spark driver */
  val PROP_CLIENT_LOG_LEVEL = ("spark.ext.h2o.client.log.level", "WARN")
  /** Location of log directory for remote nodes. */
  val PROP_NODE_LOG_DIR = ("spark.ext.h2o.node.log.dir", null.asInstanceOf[String])
  /** Location of log directory for the driver instance. */
  val PROP_CLIENT_LOG_DIR = ("spark.ext.h2o.client.log.dir", defaultLogDir)
  /** Subnet selector for h2o if IP guess fail - useful if 'spark.ext.h2o.flatfile' is false
    * and we are trying to guess right IP on mi*/
  val PROP_NETWORK_MASK = ("spark.ext.h2o.network.mask", null.asInstanceOf[String])
  /** Limit for number of threads used by H2O, default -1 means unlimited */
  val PROP_NTHREADS = ("spark.ext.h2o.nthreads", -1)
  /** Disable GA tracking */
  val PROP_DISABLE_GA = ("spark.ext.h2o.disable.ga", true)
  /** Exact client port to access web UI.
    * The value `-1` means automatic search for free port starting at `spark.ext.h2o.port.base`. */
  val PROP_CLIENT_WEB_PORT = ("spark.ext.h2o.client.web.port", -1)
  /** Location of iced directory for the driver instance. */
  val PROP_CLIENT_ICED_DIR = ("spark.ext.h2o.client.iced.dir", null.asInstanceOf[String])
  /** Configuration property - base port used for individual H2O nodes configuration. */
  val PROP_NODE_PORT_BASE = ( "spark.ext.h2o.node.port.base", 54321 )
  /** FIXME: documentation UPDATE */
  val PROP_CLIENT_PORT_BASE = ( "spark.ext.h2o.client.port.base", 54321 )
  val PROP_CLIENT_IP = ("spark.ext.h2o.client.ip", null.asInstanceOf[String])

  /** Location of iced directory for Spark nodes */
  val PROP_NODE_ICED_DIR = ("spark.ext.h2o.node.iced.dir", null.asInstanceOf[String])

  /** Configuration property - multiplication factor for dummy RDD generation.
    * Size of dummy RDD is PROP_CLUSTER_SIZE*PROP_DUMMY_RDD_MUL_FACTOR */
  val PROP_DUMMY_RDD_MUL_FACTOR = ("spark.ext.h2o.dummy.rdd.mul.factor", 10)

  /** Path to Java KeyStore file. */
  val PROP_JKS = ("spark.ext.h2o.jks", null.asInstanceOf[String])
  /** Password for Java KeyStore file. */
  val PROP_JKS_PASS = ("spark.ext.h2o.jks.pass", null.asInstanceOf[String])
  /** Enable hash login. */
  val PROP_HASH_LOGIN = ("spark.ext.h2o.hash.login", false)
  /** Enable LDAP login. */
  val PROP_LDAP_LOGIN = ("spark.ext.h2o.ldap.login", false)
  /** Login configuration file. */
  val PROP_LOGIN_CONF = ("spark.ext.h2o.login.conf", null.asInstanceOf[String])
  /** Override user name for cluster. */
  val PROP_USER_NAME = ("spark.ext.h2o.user.name", null.asInstanceOf[String])
  /** Subsequent successful tries to figure out size of Spark cluster which are producing same number of nodes. */
  val PROP_SUBSEQ_TRIES = ("spark.ext.h2o.subseq.tries", 5)
  /** Number of executors started at the start of h2o services, by default 1 */
  val PROP_SCALA_INT_DEFAULT_NUM = ("spark.ext.scala.int.default.num",1)
  /** Enable/Disable Sparkling-Water REPL **/
  val PROP_REPL_ENABLED = ("spark.ext.h2o.repl.enabled",true)

  private[spark] def defaultLogDir: String = {
    System.getProperty("user.dir") + java.io.File.separator + "h2ologs"
  }
}
