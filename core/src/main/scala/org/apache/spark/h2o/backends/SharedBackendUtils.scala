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
import org.apache.spark.h2o.{H2OConf, H2OLogging}

/**
  * Shared functions which can be used by both backends
  */
private[backends] trait SharedBackendUtils extends H2OLogging with Serializable{

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

    // Increase locality timeout since h2o-specific tasks can be long computing
    if (conf.getInt("spark.locality.wait", 3000) <= 3000) {
      logWarning(s"Increasing 'spark.locality.wait' to value 30000")
      conf.set("spark.locality.wait", "30000")
    }

    if(conf.clientIp.isEmpty){
      conf.setClientIp(getHostname(SparkEnv.get))
    }

    if(conf.backendClusterMode != "internal" && conf.backendClusterMode != "external"){
      logWarning(
        s"""'spark.ext.h2o.backend.cluster.mode' property is set to ${conf.backendClusterMode}.
          Valid options are "internal" or "external". Running in internal cluster mode now!
      """)
    }

    if (conf.getInt("spark.sql.autoBroadcastJoinThreshold", 0) != -1) {
      logWarning("Due to non-deterministic behavior of Spark broadcast-based joins\n" +
                 "We recommend to disable them by\n" +
                 "configuring `spark.sql.autoBroadcastJoinThreshold` variable to value `-1`:\n" +
                 "sqlContext.sql(\"SET spark.sql.autoBroadcastJoinThreshold=-1\")")
    }

    conf
  }

  /**
    * Get H2O arguments which are passed to every node - regular node, client node
    *
    * @param conf H2O Configuration
    * @return sequence of arguments
    */
  def getH2OCommonArgs(conf: H2OConf): Seq[String] =
  // Options in form key=value
    Seq(
      ("-name", conf.cloudName.get),
      ("-nthreads", if (conf.nthreads > 0) conf.nthreads else null),
      ("-internal_security_conf", conf.sslConf.orNull)
    ).filter(x => x._2 != null)
      .flatMap(x => Seq(x._1, x._2.toString)) ++ // Append single boolean options
      Seq(("-ga_opt_out", conf.disableGA))
        .filter(_._2).map(x => x._1)


  /**
    * Get common arguments for H2O client.
    *
    * @return array of H2O client arguments.
    */
  def getH2OClientArgs(conf: H2OConf): Array[String] = (
    getH2OCommonArgs(conf)
      ++ (if (!conf.clientVerboseOutput) Seq("-quiet") else Nil)
      ++ (if (conf.hashLogin) Seq("-hash_login") else Nil)
      ++ (if (conf.ldapLogin) Seq("-ldap_login") else Nil)
      ++ (if (conf.kerberosLogin) Seq("-kerberos_login") else Nil)
      ++ Seq("-log_level", if (conf.clientVerboseOutput) incLogLevel(conf.h2oClientLogLevel, "INFO") else conf.h2oClientLogLevel)
      ++ Seq("-log_dir", conf.h2oClientLogDir)
      ++ Seq("-baseport", conf.clientBasePort.toString)
      ++ Seq("-client")
      ++ Seq(
      ("-ice_root", conf.clientIcedDir.orNull),
      ("-port", if (conf.clientWebPort > 0) conf.clientWebPort else null),
      ("-jks", conf.jks.orNull),
      ("-jks_pass", conf.jksPass.orNull),
      ("-login_conf", conf.loginConf.orNull),
      ("-user_name", conf.userName.orNull),
      conf.clientNetworkMask.map(mask => ("-network", mask)).getOrElse(("-ip", conf.clientIp.get))
    ).filter(_._2 != null).flatMap(x => Seq(x._1, x._2.toString))
    ).toArray

  val TEMP_DIR_ATTEMPTS = 1000

  private def createTempDir(): File = {
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
    * @param logLevel  actual log level
    * @param minLogLevel  desired minimal log level
    * @return if logLevel is less verbose than minLogLeve then minLogLevel, else logLevel
    */
  private def incLogLevel(logLevel: String, minLogLevel: String): String = {
    val logLevels = Seq( ("OFF", 0), ("FATAL", 1), ("ERROR", 2),
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
