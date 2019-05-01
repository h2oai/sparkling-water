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

package org.apache.spark.h2o.backends

import java.io.File

import org.apache.spark.SparkEnv
import org.apache.spark.h2o.H2OConf
import org.apache.spark.internal.Logging

/**
  * Shared functions which can be used by both backends
  */
private[backends] trait SharedBackendUtils extends Logging with Serializable {

  /**
    * Return hostname of this node based on SparkEnv
    *
    * @param env SparkEnv instance
    * @return hostname of the node
    */
  def getHostname(env: SparkEnv) = env.blockManager.blockManagerId.host

  /** Check Spark and H2O environment, update it if necessary and and warn about possible problems.
    *
    * This method checks the environments for generic configuration which does not depend on particular backend used
    * In order to check the configuration for specific backend, method checkAndUpdateConf on particular backend has to be
    * called.
    *
    * This method has to be called at the start of each method which override this one
    *
    * @param conf H2O Configuration to check
    * @return checked and updated configuration
    * */
  def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    // Note: updating Spark Conf is useless at this time in more of the cases since SparkContext is already running
    if (conf.h2oClientLogDir.isEmpty) {
      conf.setH2OClientLogDir(defaultLogDir(conf.sparkConf.getAppId))
    }

    if (conf.backendClusterMode != "internal" && conf.backendClusterMode != "external") {
      throw new IllegalArgumentException(
        s"""'${SharedBackendConf.PROP_BACKEND_CLUSTER_MODE._1}' property is set to ${conf.backendClusterMode}.
          Valid options are "${SharedBackendConf.BACKEND_MODE_INTERNAL}" or "${SharedBackendConf.BACKEND_MODE_EXTERNAL}".
      """)
    }

    if (conf.getInt("spark.sql.autoBroadcastJoinThreshold", 0) != -1) {
      logWarning(
        """To avoid non-deterministic behavior of Spark broadcast-based joins,
          |we recommend to set `spark.sql.autoBroadcastJoinThreshold` property of SparkSession to -1.
          |E.g. spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
          |We also recommend to avoid using broadcast hints in your Spark SQL code.""".stripMargin)
    }

    if (conf.contextPath.isDefined) {
      if (!conf.contextPath.get.startsWith("/")) {
        logWarning("Context path does not start with mandatory \"/\", appending it.")
        conf.setContextPath("/" + conf.contextPath.get)
      } else {
        // Check if it starts with exactly one "/"
        val pattern = "^//(/*)".r
        if (pattern.findFirstIn(conf.contextPath.get).isDefined) {
          logWarning("Context path contains multiple starting \"/\", it can contain only one. Replacing with one" +
            " slash")
          conf.setContextPath(pattern.replaceFirstIn(conf.contextPath.get, "/"))
        }
      }
    }

    conf
  }

  def defaultLogDir(appId: String): String = {
    System.getProperty("user.dir") + java.io.File.separator + "h2ologs" + File.separator + appId
  }

  def addIfNotNull(arg: String, value: String) = if (value != null) Seq(arg, value.toString) else Nil


  /**
    * Get H2O arguments which are passed to every node - regular node, client node
    *
    * @param conf H2O Configuration
    * @return sequence of arguments
    */
  def getH2OCommonArgs(conf: H2OConf): Seq[String] = (
    // Options in form key=value
    Seq("-name", conf.cloudName.get, "-port_offset", conf.internalPortOffset.toString)
      ++ addIfNotNull("-stacktrace_collector_interval", Some(conf.stacktraceCollectorInterval).filter(_ > 0).map(_.toString).orNull)
      ++ addIfNotNull("-nthreads", Some(conf.nthreads).filter(_ > 0).map(_.toString).orElse(conf.sparkConf.getOption("spark.executor.cores")).orNull)
      ++ addIfNotNull("-internal_security_conf", conf.sslConf.orNull)
    )

  def getLoginArgs(conf: H2OConf): Seq[String] = {
    val base = (
      (if (conf.hashLogin) Seq("-hash_login") else Nil)
        ++ (if (conf.ldapLogin) Seq("-ldap_login") else Nil)
        ++ (if (conf.kerberosLogin) Seq("-kerberos_login") else Nil)
        ++ addIfNotNull("-user_name", conf.userName.orNull)
        ++ addIfNotNull("-login_conf", conf.loginConf.orNull)
      )

    if (!conf.clientWebEnabled) {
      val f = new File(SharedBackendUtils.createTempDir(), "dummy")
      f.createNewFile()
      f.deleteOnExit()
      base ++ Seq("-hash_login", "-login_conf", f.toString)
    }
    else {
      base
    }
  }

  def getH2OClientArgsLocalNode(conf: H2OConf): Array[String] = (
    getH2OCommonArgs(conf) ++ getLoginArgs(conf)
      ++ (if (!conf.clientVerboseOutput) Seq("-quiet") else Nil)
      ++ Seq("-log_level", if (conf.clientVerboseOutput) incLogLevel(conf.h2oClientLogLevel, "INFO") else conf.h2oClientLogLevel)
      ++ Seq("-log_dir", conf.h2oClientLogDir.get)
      ++ Seq("-baseport", conf.clientBasePort.toString)
      ++ addIfNotNull("-context_path", conf.contextPath.orNull)
      ++ addIfNotNull("-flow_dir", conf.flowDir.orNull)
      ++ addIfNotNull("-ice_root", conf.clientIcedDir.orNull)
      ++ addIfNotNull("-port", Some(conf.clientWebPort).filter(_ > 0).map(_.toString).orNull)
      ++ addIfNotNull("-jks", conf.jks.orNull)
      ++ addIfNotNull("-jks_pass", conf.jksPass.orNull)
      ++ conf.clientNetworkMask.map(mask => Seq("-network", mask)).getOrElse(Seq("-ip", conf.clientIp.get))
    ).toArray

  /**
    * Get common arguments for H2O client.
    *
    * @return array of H2O client arguments.
    */
  def getH2OClientArgs(conf: H2OConf): Array[String] = getH2OClientArgsLocalNode(conf) ++ Seq("-client")


  val TEMP_DIR_ATTEMPTS = 1000

  def createTempDir(): File = {
    def baseDir = new File(System.getProperty("java.io.tmpdir"))

    def baseName = System.currentTimeMillis() + "-"

    var cnt = 0
    while (cnt < TEMP_DIR_ATTEMPTS) {
      // infinite loop
      val tempDir = new File(baseDir, baseName + cnt)
      if (tempDir.mkdir()) return tempDir
      cnt += 1
    }
    throw new IllegalStateException(s"Failed to create temporary directory ${baseDir} / ${baseName}")
  }

  def saveAsFile(content: String): File = {
    val tmpDir = createTempDir()
    tmpDir.deleteOnExit()
    val flatFile = new File(tmpDir, "flatfile.txt")
    val p = new java.io.PrintWriter(flatFile)
    try {
      p.print(content)
    } finally {
      p.close()
    }
    flatFile
  }

  /**
    * Increment log level to at least desired minimal log level.
    *
    * @param logLevel    actual log level
    * @param minLogLevel desired minimal log level
    * @return if logLevel is less verbose than minLogLeve then minLogLevel, else logLevel
    */
  private def incLogLevel(logLevel: String, minLogLevel: String): String = {
    val logLevels = Seq(("OFF", 0), ("FATAL", 1), ("ERROR", 2),
      ("WARN", 3), ("INFO", 4), ("DEBUG", 5), ("TRACE", 6), ("ALL", 7))
    val ll = logLevels.find(t => t._1 == logLevel)
    val mll = logLevels.find(t => t._1 == minLogLevel)
    if (mll.isEmpty) {
      logLevel
    } else {
      ll.map(v => if (v._2 < mll.get._2) minLogLevel else logLevel).getOrElse(minLogLevel)
    }
  }
}

object SharedBackendUtils extends SharedBackendUtils
