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

package ai.h2o.sparkling.backend.utils

import java.io.File

import ai.h2o.sparkling.backend.SharedBackendConf
import org.apache.spark.expose.{Logging, Utils}
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.{SparkContext, SparkEnv, SparkFiles}
import water.support.SparkContextSupport

/**
 * Shared functions which can be used by both backends
 */
trait SharedBackendUtils extends Logging with Serializable {

  /**
   * Return hostname of this node based on SparkEnv
   *
   * @param env SparkEnv instance
   * @return hostname of the node
   */
  def getHostname(env: SparkEnv): String = env.blockManager.blockManagerId.host

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

    if (AzureDatabricksUtils.isRunningOnAzureDatabricks(conf)) {
      AzureDatabricksUtils.setClientWebPort(conf)
      AzureDatabricksUtils.setClientCheckRetryTimeout(conf)
    }

    if (conf.isInternalSecureConnectionsEnabled && conf.sslConf.isEmpty) {
      SecurityUtils.enableSSL(conf)
    }

    if (conf.autoFlowSsl) {
      SecurityUtils.enableFlowSSL(conf)
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

    if (conf.clientVerboseOutput) {
      conf.setH2OClientLogLevel(incLogLevel(conf.h2oClientLogLevel, "INFO"))
    }

    conf
  }

  def distributeFiles(conf: H2OConf, sc: SparkContext): Unit = {
    for (fileProperty <- conf.getFileProperties) {
      for (filePath <- conf.getOption(fileProperty._1)) {
        if (!SparkContextSupport.isFileDistributed(sc, filePath)) {
          sc.addFile(filePath)
        }
      }
    }
  }


  def defaultLogDir(appId: String): String = {
    System.getProperty("user.dir") + java.io.File.separator + "h2ologs" + File.separator + appId
  }

  def getH2OSecurityArgs(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add("-jks", getDistributedFilePath(conf.jks))
      .add("-jks_pass", conf.jksPass)
      .add("-jks_alias", conf.jksAlias)
      .addIf("-hash_login", conf.hashLogin)
      .addIf("-ldap_login", conf.ldapLogin)
      .addIf("-kerberos_login", conf.kerberosLogin)
      .add("-user_name", conf.userName)
      .add("-login_conf", getDistributedFilePath(conf.loginConf))
      .add("-internal_security_conf", getDistributedFilePath(conf.sslConf))
      .buildArgs()
  }

  /**
   * Get H2O arguments which are passed to every node - regular node, client node
   *
   * @param conf H2O Configuration
   * @return sequence of arguments
   */
  def getH2OCommonArgs(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add("-allow_clients", !RestApiUtils.isRestAPIBased(conf))
      .add("-internal_security_conf_rel_paths")
      .add("-name", conf.cloudName.get)
      .add("-port_offset", conf.internalPortOffset)
      .add("-stacktrace_collector_interval", Some(conf.stacktraceCollectorInterval).filter(_ > 0))
      .add("-nthreads", Some(conf.nthreads).filter(_ > 0).orElse(conf.sparkConf.getOption("spark.executor.cores")))
      .add("-client_disconnect_timeout", conf.clientCheckRetryTimeout)
      .add("-hdfs_config", getDistributedFilePath(conf.hdfsConf))
      .add(getExtraHttpHeaderArgs(conf))
      .add("-embedded")
      .buildArgs()
  }

  def getH2OWorkerAsClientArgs(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add(getH2OCommonArgs(conf))
      .add(getH2OSecurityArgs(conf))
      .addIf("-quiet", !conf.clientVerboseOutput)
      .add("-log_level", conf.h2oClientLogLevel)
      .add("-log_dir", conf.h2oClientLogDir)
      .add("-baseport", conf.clientBasePort)
      .add("-context_path", conf.contextPath)
      .add("-flow_dir", conf.flowDir)
      .add("-ice_root", conf.clientIcedDir)
      .add("-port", Some(conf.clientWebPort).filter(_ > 0))
      .add("-network", conf.clientNetworkMask)
      .addIf("-ip", conf.clientIp, conf.clientNetworkMask.isEmpty)
      .addAsString(conf.clientExtraProperties)
      .buildArgs()
  }

  def parseStringToHttpHeaderArgs(headers: String): Seq[String] = {
    val headerPattern = """^\s*([^:]+)\:\s*(.+)$""".r
    headers.split('\n').flatMap {
      case headerPattern(key, value) => Seq("-add_http_header", key, value)
      case _ => Seq.empty
    }
  }

  def getExtraHttpHeaderArgs(conf: H2OConf): Seq[String] = {
    conf.flowExtraHttpHeaders.map(parseStringToHttpHeaderArgs).getOrElse(Seq.empty)
  }

  def toH2OArgs(h2oArgs: Seq[String], executors: Array[NodeDesc] = Array()): Array[String] = {
    val flatFileString = toFlatFileString(executors)
    val flatFile = saveFlatFileAsFile(flatFileString)
    h2oArgs.toArray ++ Array("-flatfile", flatFile.getAbsolutePath)
  }

  def createTempDir(): File = {
    val sparkLocalDir = Utils.getLocalDir(SparkEnv.get.conf)
    Utils.createTempDir(sparkLocalDir, "sparkling-water")
  }

  def saveFlatFileAsFile(content: String): File = {
    val tmpDir = createTempDir()
    val flatFile = new File(tmpDir, "flatfile.txt")
    val p = new java.io.PrintWriter(flatFile)
    try {
      p.print(content)
    } finally {
      p.close()
    }
    flatFile
  }

  protected def translateHostnameToIp(hostname: String): String = {
    import java.net.InetAddress
    InetAddress.getByName(hostname).getHostAddress
  }

  private def getDistributedFilePath(fileConf: Option[String]): Option[String] = {
    fileConf.map(name => SparkFiles.get(new File(name).getName))
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

  private def toFlatFileString(executors: Array[NodeDesc]): String = {
    executors.map {
      en => s"${translateHostnameToIp(en.hostname)}:${en.port}"
    }.mkString("\n")
  }

}
