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
import java.nio.file.Paths

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.backend.{NodeDesc, SharedBackendConf}
import org.apache.spark.expose.{Logging, Utils}
import org.apache.spark.{SparkContext, SparkEnv, SparkFiles}

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
    */
  def checkAndUpdateConf(conf: H2OConf): H2OConf = {
    // Note: updating Spark Conf is useless at this time in more of the cases since SparkContext is already running

    if (AzureDatabricksUtils.isRunningOnAzureDatabricks(conf)) {
      AzureDatabricksUtils.setClientWebPort(conf)
      AzureDatabricksUtils.setClientCheckRetryTimeout(conf)
    }

    if (conf.isInternalSecureConnectionsEnabled && conf.sslConf.isEmpty) {
      SecurityUtils.enableSSL(conf)
    }

    if (conf.jks.isDefined) {
      System.setProperty("javax.net.ssl.trustStore", conf.jks.get)
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

    if (conf.contextPath.isDefined) {
      if (!conf.contextPath.get.startsWith("/")) {
        logWarning("Context path does not start with mandatory \"/\", appending it.")
        conf.setContextPath("/" + conf.contextPath.get)
      } else {
        // Check if it starts with exactly one "/"
        val pattern = "^//(/*)".r
        if (pattern.findFirstIn(conf.contextPath.get).isDefined) {
          logWarning(
            "Context path contains multiple starting \"/\", it can contain only one. Replacing with one" +
              " slash")
          conf.setContextPath(pattern.replaceFirstIn(conf.contextPath.get, "/"))
        }
      }
    }

    if (conf.clientVerboseOutput) {
      conf.setLogLevel(incLogLevel(conf.logLevel, "INFO"))
    }

    if (conf.isKerberizedHiveEnabled) {
      if (conf.hiveHost.isEmpty && conf.hiveJdbcUrlPattern.isEmpty) {
        throw new IllegalArgumentException(
          s"When Hive support is enabled, the option '${SharedBackendConf.PROP_HIVE_HOST._1}' " +
            s"or '${SharedBackendConf.PROP_HIVE_JDBC_URL_PATTERN._1}' must be defined.")
      }
      if (conf.hivePrincipal.isEmpty) {
        throw new IllegalArgumentException(
          s"When Hive support is enabled, the option '${SharedBackendConf.PROP_HIVE_PRINCIPAL._1}' must be defined.")
      }
      if (conf.hiveToken.isEmpty) {
        throw new IllegalArgumentException(
          s"When Hive support is enabled, the option '${SharedBackendConf.PROP_HIVE_TOKEN._1}' must be defined.")
      }
    }

    conf
  }

  def distributeFiles(conf: H2OConf, sc: SparkContext): Unit = {
    for (fileProperty <- conf.getFileProperties) {
      for (filePath <- conf.getOption(fileProperty._1)) {
        if (!isFileDistributed(sc, filePath)) {
          sc.addFile(filePath)
        }
      }
    }
  }

  /**
    * Returns true if the file has already been added to Spark files
    */
  private def isFileDistributed(sc: SparkContext, filePath: String): Boolean = {
    val fileName = new File(filePath).getName
    sc.listFiles().exists(new File(_).getName == fileName)
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
      .add("-allow_insecure_xgboost", conf.isInsecureXGBoostAllowed)
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
      .add(H2OClientUtils.getH2OCommonArgsWhenClientBased(conf), H2OClientUtils.isH2OClientBased(conf))
      .add("-internal_security_conf_rel_paths")
      .add("-name", conf.cloudName.get)
      .add("-port_offset", conf.internalPortOffset)
      .add("-context_path", conf.contextPath)
      .add("-stacktrace_collector_interval", Some(conf.stacktraceCollectorInterval).filter(_ > 0))
      .add("-nthreads", Some(conf.nthreads).filter(_ > 0).orElse(conf.sparkConf.getOption("spark.executor.cores")))
      .add("-hdfs_config", getDistributedFilePath(conf.hdfsConf))
      .add(getExtraHttpHeaderArgs(conf))
      .add("-log_level", conf.logLevel)
      .add("-embedded")
      .add("-baseport", conf.basePort)
      .add("-log_dir", determineLogDir(conf))
      .add("-ice_root", conf.icedDir)
      .add("-flow_dir", conf.flowDir)
      .addAsString(conf.extraProperties)
      .buildArgs()
  }

  def getH2OWorkerAsClientArgs(conf: H2OConf): Seq[String] = {
    new ArgumentBuilder()
      .add(getH2OCommonArgs(conf))
      .add(getH2OSecurityArgs(conf))
      .addIf("-quiet", !conf.clientVerboseOutput)
      .add("-port", Some(conf.clientWebPort).filter(_ > 0))
      .buildArgs()
  }

  private def determineLogDir(conf: H2OConf): String = {
    Option(System.getProperty("spark.yarn.app.container.log.dir"))
      .map(_ + java.io.File.separator)
      .orElse(conf.logDir)
      .getOrElse(
        Paths.get(System.getProperty("user.dir"), "h2ologs", SparkEnv.get.conf.getAppId).toAbsolutePath.toString)
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
    val logLevels =
      Seq(("OFF", 0), ("FATAL", 1), ("ERROR", 2), ("WARN", 3), ("INFO", 4), ("DEBUG", 5), ("TRACE", 6), ("ALL", 7))
    val ll = logLevels.find(t => t._1 == logLevel)
    val mll = logLevels.find(t => t._1 == minLogLevel)
    if (mll.isEmpty) {
      logLevel
    } else {
      ll.map(v => if (v._2 < mll.get._2) minLogLevel else logLevel).getOrElse(minLogLevel)
    }
  }

  private def toFlatFileString(executors: Array[NodeDesc]): String = {
    executors
      .map { en =>
        s"${translateHostnameToIp(en.hostname)}:${en.port}"
      }
      .mkString("\n")
  }
}
