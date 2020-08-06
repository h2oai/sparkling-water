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

package ai.h2o.sparkling.backend

import ai.h2o.sparkling.H2OConf
import ai.h2o.sparkling.H2OConf.{BooleanOption, IntOption, OptionOption, StringOption}
import ai.h2o.sparkling.macros.DeprecatedMethod

import scala.collection.JavaConverters._

/**
  * Shared configuration independent on used backend
  */
trait SharedBackendConf extends SharedBackendConfExtensions {
  self: H2OConf =>

  import SharedBackendConf._

  /** Getters */
  def backendClusterMode: String = sparkConf.get(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2)

  def cloudName: Option[String] = sparkConf.getOption(PROP_CLOUD_NAME._1)

  def nthreads: Int = sparkConf.getInt(PROP_NTHREADS._1, PROP_NTHREADS._2)

  def isH2OReplEnabled: Boolean = sparkConf.getBoolean(PROP_REPL_ENABLED._1, PROP_REPL_ENABLED._2)

  def scalaIntDefaultNum: Int = sparkConf.getInt(PROP_SCALA_INT_DEFAULT_NUM._1, PROP_SCALA_INT_DEFAULT_NUM._2)

  def isClusterTopologyListenerEnabled: Boolean =
    sparkConf.getBoolean(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._2)

  def isSparkVersionCheckEnabled: Boolean =
    sparkConf.getBoolean(PROP_SPARK_VERSION_CHECK_ENABLED._1, PROP_SPARK_VERSION_CHECK_ENABLED._2)

  def isFailOnUnsupportedSparkParamEnabled: Boolean =
    sparkConf.getBoolean(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._2)

  def jks: Option[String] = sparkConf.getOption(PROP_JKS._1)

  def jksPass: Option[String] = sparkConf.getOption(PROP_JKS_PASS._1)

  def jksAlias: Option[String] = sparkConf.getOption(PROP_JKS_ALIAS._1)

  def hashLogin: Boolean = sparkConf.getBoolean(PROP_HASH_LOGIN._1, PROP_HASH_LOGIN._2)

  def ldapLogin: Boolean = sparkConf.getBoolean(PROP_LDAP_LOGIN._1, PROP_LDAP_LOGIN._2)

  def kerberosLogin: Boolean = sparkConf.getBoolean(PROP_KERBEROS_LOGIN._1, PROP_KERBEROS_LOGIN._2)

  def loginConf: Option[String] = sparkConf.getOption(PROP_LOGIN_CONF._1)

  def userName: Option[String] = sparkConf.getOption(PROP_USER_NAME._1)

  def password: Option[String] = sparkConf.getOption(PROP_PASSWORD._1)

  def sslConf: Option[String] = sparkConf.getOption(PROP_SSL_CONF._1)

  def autoFlowSsl: Boolean = sparkConf.getBoolean(PROP_AUTO_SSL_FLOW._1, PROP_AUTO_SSL_FLOW._2)

  @DeprecatedMethod("logLevel", "3.34")
  def h2oNodeLogLevel: String = logLevel

  def logLevel: String = sparkConf.get(PROP_LOG_LEVEL._1, PROP_LOG_LEVEL._2)

  @DeprecatedMethod("logDir", "3.34")
  def h2oNodeLogDir: Option[String] = logDir

  def logDir: Option[String] = sparkConf.getOption(PROP_LOG_DIR._1)

  def backendHeartbeatInterval: Int =
    sparkConf.getInt(PROP_BACKEND_HEARTBEAT_INTERVAL._1, PROP_BACKEND_HEARTBEAT_INTERVAL._2)

  def cloudTimeout: Int = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)

  def nodeNetworkMask: Option[String] = sparkConf.getOption(PROP_NODE_NETWORK_MASK._1)

  def stacktraceCollectorInterval: Int =
    sparkConf.getInt(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._2)

  def contextPath: Option[String] = sparkConf.getOption(PROP_CONTEXT_PATH._1)

  def flowScalaCellAsync: Boolean = sparkConf.getBoolean(PROP_FLOW_SCALA_CELL_ASYNC._1, PROP_FLOW_SCALA_CELL_ASYNC._2)

  def maxParallelScalaCellJobs: Int =
    sparkConf.getInt(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, PROP_FLOW_SCALA_CELL_MAX_PARALLEL._2)

  def internalPortOffset: Int = sparkConf.getInt(PROP_INTERNAL_PORT_OFFSET._1, PROP_INTERNAL_PORT_OFFSET._2)

  @DeprecatedMethod("basePort", "3.34")
  def nodeBasePort: Int = basePort

  def basePort: Int = sparkConf.getInt(PROP_BASE_PORT._1, PROP_BASE_PORT._2)

  def mojoDestroyTimeout: Int = sparkConf.getInt(PROP_MOJO_DESTROY_TIMEOUT._1, PROP_MOJO_DESTROY_TIMEOUT._2)

  @DeprecatedMethod("extraProperties", "3.34")
  def nodeExtraProperties: Option[String] = extraProperties

  def extraProperties: Option[String] = sparkConf.getOption(PROP_EXTRA_PROPERTIES._1)

  def flowExtraHttpHeaders: Option[String] = sparkConf.getOption(PROP_FLOW_EXTRA_HTTP_HEADERS._1)

  def isInternalSecureConnectionsEnabled: Boolean =
    sparkConf.getBoolean(PROP_INTERNAL_SECURE_CONNECTIONS._1, PROP_INTERNAL_SECURE_CONNECTIONS._2)

  def isInsecureXGBoostAllowed: Boolean =
    sparkConf.getBoolean(PROP_ALLOW_INSECURE_XGBOOST._1, PROP_ALLOW_INSECURE_XGBOOST._2)

  def flowDir: Option[String] = sparkConf.getOption(PROP_FLOW_DIR._1)

  def clientIp: Option[String] = sparkConf.getOption(PROP_CLIENT_IP._1)

  @DeprecatedMethod("icedDir", "3.34")
  def clientIcedDir: Option[String] = icedDir

  @DeprecatedMethod("logLevel", "3.34")
  def h2oClientLogLevel: String = logLevel

  @DeprecatedMethod("logDir", "3.34")
  def h2oClientLogDir: Option[String] = logDir

  @DeprecatedMethod("basePort", "3.34")
  def clientBasePort: Int = basePort

  def clientWebPort: Int = sparkConf.getInt(PROP_CLIENT_WEB_PORT._1, PROP_CLIENT_WEB_PORT._2)

  def clientVerboseOutput: Boolean = sparkConf.getBoolean(PROP_CLIENT_VERBOSE._1, PROP_CLIENT_VERBOSE._2)

  def clientNetworkMask: Option[String] = sparkConf.getOption(PROP_CLIENT_NETWORK_MASK._1)

  def clientFlowBaseurlOverride: Option[String] = sparkConf.getOption(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1)

  @DeprecatedMethod("extraProperties", "3.34")
  def clientExtraProperties: Option[String] = extraProperties

  def runsInExternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_EXTERNAL

  def runsInInternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_INTERNAL

  def clientCheckRetryTimeout: Int =
    sparkConf.getInt(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2)

  def verifySslCertificates: Boolean =
    sparkConf.getBoolean(PROP_VERIFY_SSL_CERTIFICATES._1, PROP_VERIFY_SSL_CERTIFICATES._2)

  def isKerberizedHiveEnabled: Boolean =
    sparkConf.getBoolean(PROP_KERBERIZED_HIVE_ENABLED._1, PROP_KERBERIZED_HIVE_ENABLED._2)

  def hiveHost: Option[String] = sparkConf.getOption(PROP_HIVE_HOST._1)

  def hivePrincipal: Option[String] = sparkConf.getOption(PROP_HIVE_PRINCIPAL._1)

  def hiveJdbcUrlPattern: Option[String] = sparkConf.getOption(PROP_HIVE_JDBC_URL_PATTERN._1)

  def hiveToken: Option[String] = sparkConf.getOption(PROP_HIVE_TOKEN._1)

  def icedDir: Option[String] = sparkConf.getOption(PROP_ICED_DIR._1)

  def restApiTimeout: Int = sparkConf.getInt(PROP_REST_API_TIMEOUT._1, PROP_REST_API_TIMEOUT._2)

  /** Setters */
  def setInternalClusterMode(): H2OConf = {
    if (runsInExternalClusterMode) {
      logWarning("Using internal cluster mode!")
    }
    setBackendClusterMode(BACKEND_MODE_INTERNAL)
  }

  def setExternalClusterMode(): H2OConf = {
    if (runsInInternalClusterMode) {
      logWarning("Using external cluster mode!")
    }
    setBackendClusterMode(BACKEND_MODE_EXTERNAL)
  }

  def setCloudName(cloudName: String): H2OConf = set(PROP_CLOUD_NAME._1, cloudName)

  def setNthreads(numThreads: Int): H2OConf = set(PROP_NTHREADS._1, numThreads.toString)

  def setReplEnabled(): H2OConf = set(PROP_REPL_ENABLED._1, value = true)

  def setReplDisabled(): H2OConf = set(PROP_REPL_ENABLED._1, value = false)

  def setDefaultNumReplSessions(numSessions: Int): H2OConf = set(PROP_SCALA_INT_DEFAULT_NUM._1, numSessions.toString)

  def setClusterTopologyListenerEnabled(): H2OConf = set(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, value = true)

  def setClusterTopologyListenerDisabled(): H2OConf = set(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, value = false)

  def setSparkVersionCheckEnabled(): H2OConf = set(PROP_SPARK_VERSION_CHECK_ENABLED._1, value = true)

  def setSparkVersionCheckDisabled(): H2OConf = set(PROP_SPARK_VERSION_CHECK_ENABLED._1, value = false)

  def setFailOnUnsupportedSparkParamEnabled(): H2OConf = set(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, value = true)

  def setFailOnUnsupportedSparkParamDisabled(): H2OConf = set(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, value = false)

  def setJks(path: String): H2OConf = set(PROP_JKS._1, path)

  def setJksPass(password: String): H2OConf = set(PROP_JKS_PASS._1, password)

  def setJksAlias(alias: String): H2OConf = set(PROP_JKS_ALIAS._1, alias)

  def setHashLoginEnabled(): H2OConf = set(PROP_HASH_LOGIN._1, value = true)

  def setHashLoginDisabled(): H2OConf = set(PROP_HASH_LOGIN._1, value = false)

  def setLdapLoginEnabled(): H2OConf = set(PROP_LDAP_LOGIN._1, value = true)

  def setLdapLoginDisabled(): H2OConf = set(PROP_LDAP_LOGIN._1, value = false)

  def setKerberosLoginEnabled(): H2OConf = set(PROP_KERBEROS_LOGIN._1, value = true)

  def setKerberosLoginDisabled(): H2OConf = set(PROP_KERBEROS_LOGIN._1, value = false)

  def setLoginConf(filePath: String): H2OConf = set(PROP_LOGIN_CONF._1, filePath)

  def setUserName(username: String): H2OConf = set(PROP_USER_NAME._1, username)

  def setPassword(password: String): H2OConf = set(PROP_PASSWORD._1, password)

  def setSslConf(path: String): H2OConf = set(PROP_SSL_CONF._1, path)

  def setAutoFlowSslEnabled(): H2OConf = set(PROP_AUTO_SSL_FLOW._1, value = true)

  def setAutoFlowSslDisabled(): H2OConf = set(PROP_AUTO_SSL_FLOW._1, value = false)

  @DeprecatedMethod("setLogLevel", "3.34")
  def setH2ONodeLogLevel(level: String): H2OConf = setLogLevel(level)

  def setLogLevel(level: String): H2OConf = set(PROP_LOG_LEVEL._1, level)

  @DeprecatedMethod("setLogDir", "3.34")
  def setH2ONodeLogDir(dir: String): H2OConf = set(PROP_LOG_DIR._1, dir)

  def setLogDir(dir: String): H2OConf = set(PROP_LOG_DIR._1, dir)

  def setBackendHeartbeatInterval(interval: Int): H2OConf = set(PROP_BACKEND_HEARTBEAT_INTERVAL._1, interval.toString)

  def setCloudTimeout(timeout: Int): H2OConf = set(PROP_CLOUD_TIMEOUT._1, timeout.toString)

  def setNodeNetworkMask(mask: String): H2OConf = set(PROP_NODE_NETWORK_MASK._1, mask)

  def setStacktraceCollectorInterval(interval: Int): H2OConf =
    set(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, interval.toString)

  def setContextPath(contextPath: String): H2OConf = set(PROP_CONTEXT_PATH._1, contextPath)

  def setFlowScalaCellAsyncEnabled(): H2OConf = set(PROP_FLOW_SCALA_CELL_ASYNC._1, value = true)

  def setFlowScalaCellAsyncDisabled(): H2OConf = set(PROP_FLOW_SCALA_CELL_ASYNC._1, value = false)

  def setMaxParallelScalaCellJobs(limit: Int): H2OConf = set(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, limit.toString)

  def setInternalPortOffset(offset: Int): H2OConf = set(PROP_INTERNAL_PORT_OFFSET._1, offset.toString)

  @DeprecatedMethod("setBasePort", "3.34")
  def setNodeBasePort(port: Int): H2OConf = setBasePort(port)

  def setBasePort(port: Int): H2OConf = set(PROP_BASE_PORT._1, port.toString)

  def setMojoDestroyTimeout(timeoutInMilliseconds: Int): H2OConf =
    set(PROP_MOJO_DESTROY_TIMEOUT._1, timeoutInMilliseconds.toString)

  @DeprecatedMethod("setExtraProperties", "3.34")
  def setNodeExtraProperties(extraProperties: String): H2OConf = setExtraProperties(extraProperties)

  def setExtraProperties(extraProperties: String): H2OConf = set(PROP_EXTRA_PROPERTIES._1, extraProperties)

  def setFlowExtraHttpHeaders(headers: String): H2OConf = { // R mapping
    set(PROP_FLOW_EXTRA_HTTP_HEADERS._1, headers)
  }

  def setFlowExtraHttpHeaders(headers: java.util.HashMap[String, String]): H2OConf = { // Py4J mapping
    setFlowExtraHttpHeaders(headers.asScala.toMap[String, String])
  }

  def setFlowExtraHttpHeaders(headers: Map[String, String]): H2OConf = {
    val stringRepresentation = headers.map(header => s"${header._1}: ${header._2}").mkString("\n")
    setFlowExtraHttpHeaders(stringRepresentation)
  }

  def setInternalSecureConnectionsEnabled(): H2OConf = set(PROP_INTERNAL_SECURE_CONNECTIONS._1, value = true)

  def setInternalSecureConnectionsDisabled(): H2OConf = set(PROP_INTERNAL_SECURE_CONNECTIONS._1, value = false)

  def setInsecureXGBoostAllowed(): H2OConf = set(PROP_ALLOW_INSECURE_XGBOOST._1, value = true)

  def setInsecureXGBoostDenied(): H2OConf = set(PROP_ALLOW_INSECURE_XGBOOST._1, value = false)

  def setFlowDir(dir: String): H2OConf = set(PROP_FLOW_DIR._1, dir)

  def setClientIp(ip: String): H2OConf = set(PROP_CLIENT_IP._1, ip)

  @DeprecatedMethod("setIcedDir", "3.34")
  def setClientIcedDir(icedDir: String): H2OConf = setIcedDir(icedDir)

  @DeprecatedMethod("setLogLevel", "3.34")
  def setH2OClientLogLevel(level: String): H2OConf = setLogLevel(level)

  @DeprecatedMethod("setLogDir", "3.34")
  def setH2OClientLogDir(dir: String): H2OConf = setLogDir(dir)

  @DeprecatedMethod("setBasePort", "3.34")
  def setClientBasePort(basePort: Int): H2OConf = setBasePort(basePort)

  def setClientWebPort(port: Int): H2OConf = set(PROP_CLIENT_WEB_PORT._1, port.toString)

  def setClientVerboseEnabled(): H2OConf = set(PROP_CLIENT_VERBOSE._1, value = true)

  def setClientVerboseDisabled(): H2OConf = set(PROP_CLIENT_VERBOSE._1, value = false)

  def setClientNetworkMask(mask: String): H2OConf = set(PROP_CLIENT_NETWORK_MASK._1, mask)

  def setClientFlowBaseurlOverride(baseUrl: String): H2OConf = set(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1, baseUrl)

  def setClientCheckRetryTimeout(timeout: Int): H2OConf = set(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, timeout.toString)

  @DeprecatedMethod("setExtraProperties", "3.34")
  def setClientExtraProperties(extraProperties: String): H2OConf = setExtraProperties(extraProperties)

  def setVerifySslCertificates(verify: Boolean): H2OConf = set(PROP_VERIFY_SSL_CERTIFICATES._1, verify)

  def setKerberizedHiveEnabled(): H2OConf = set(PROP_KERBERIZED_HIVE_ENABLED._1, true)

  def setKerberizedHiveDisabled(): H2OConf = set(PROP_KERBERIZED_HIVE_ENABLED._1, false)

  def setHiveHost(host: String): H2OConf = set(PROP_HIVE_HOST._1, host)

  def setHivePrincipal(principal: String): H2OConf = set(PROP_HIVE_PRINCIPAL._1, principal)

  def setHiveJdbcUrlPattern(pattern: String): H2OConf = set(PROP_HIVE_JDBC_URL_PATTERN._1, pattern)

  def setHiveToken(token: String): H2OConf = set(PROP_HIVE_TOKEN._1, token)

  def setIcedDir(dir: String): H2OConf = set(PROP_ICED_DIR._1, dir)

  def setRestApiTimeout(timeout: Int): H2OConf = set(PROP_REST_API_TIMEOUT._1, timeout.toString)
}

object SharedBackendConf {

  val BACKEND_MODE_INTERNAL: String = "internal"
  val BACKEND_MODE_EXTERNAL: String = "external"

  val PROP_BACKEND_CLUSTER_MODE: StringOption = (
    "spark.ext.h2o.backend.cluster.mode",
    BACKEND_MODE_INTERNAL,
    """setInternalClusterMode()
      |setExternalClusterMode()""".stripMargin,
    """This option can be set either to ``internal`` or ``external``. When set to ``external``, ``H2O Context`` is
      |created by connecting to existing H2O cluster, otherwise H2O cluster located inside Spark is created. That
      |means that each Spark executor will have one H2O instance running in it. The ``internal`` mode is not
      |recommended for big clusters and clusters where Spark executors are not stable.""".stripMargin)

  val PROP_CLOUD_NAME: OptionOption = (
    "spark.ext.h2o.cloud.name",
    None,
    "setCloudName(String)",
    "Name of H2O cluster. If this option is not set, the name is automatically generated")

  val PROP_NTHREADS: IntOption = (
    "spark.ext.h2o.nthreads",
    -1,
    "setNthreads(Integer)",
    """Limit for number of threads used by H2O, default ``-1`` means: Use value of ``spark.executor.cores`` in
      |case this property is set. Otherwise use H2O's default
      |value Runtime.getRuntime()
      |.availableProcessors()""".stripMargin)

  val PROP_REPL_ENABLED: BooleanOption = (
    "spark.ext.h2o.repl.enabled",
    true,
    """setReplEnabled()
      |setReplDisabled()""".stripMargin,
    "Decides whether H2O REPL is initiated or not.")

  val PROP_SCALA_INT_DEFAULT_NUM: IntOption = (
    "spark.ext.scala.int.default.num",
    1,
    "setDefaultNumReplSessions(Integer)",
    "Number of parallel REPL sessions started at the start of Sparkling Water.")

  val PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED: BooleanOption = (
    "spark.ext.h2o.topology.change.listener.enabled",
    true,
    """setClusterTopologyListenerEnabled()
      |setClusterTopologyListenerDisabled()""".stripMargin,
    """Decides whether listener which kills H2O cluster on the change of the underlying cluster's topology is
      |enabled or not. This configuration has effect only in non-local mode.""".stripMargin)

  val PROP_SPARK_VERSION_CHECK_ENABLED: BooleanOption = (
    "spark.ext.h2o.spark.version.check.enabled",
    true,
    """setSparkVersionCheckEnabled()
      |setSparkVersionCheckDisabled()""".stripMargin,
    "Enables check if run-time Spark version matches build time Spark version.")

  val PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM: BooleanOption = (
    "spark.ext.h2o.fail.on.unsupported.spark.param",
    true,
    """setFailOnUnsupportedSparkParamEnabled()
      |setFailOnUnsupportedSparkParamDisabled()""".stripMargin,
    "If unsupported Spark parameter is detected, then application is forced to shutdown.")

  val PROP_JKS: OptionOption = ("spark.ext.h2o.jks", None, "setJks(String)", "Path to Java KeyStore file.")

  val PROP_JKS_PASS: OptionOption =
    ("spark.ext.h2o.jks.pass", None, "setJksPass(String)", "Password for Java KeyStore file.")

  val PROP_JKS_ALIAS: OptionOption =
    ("spark.ext.h2o.jks.alias", None, "setJksAlias(String)", "Alias to certificate in keystore to secure H2O Flow.")

  val PROP_HASH_LOGIN: BooleanOption = (
    "spark.ext.h2o.hash.login",
    false,
    """setHashLoginEnabled()
      |setHashLoginDisabled()""".stripMargin,
    "Enable hash login.")

  val PROP_LDAP_LOGIN: BooleanOption = (
    "spark.ext.h2o.ldap.login",
    false,
    """setLdapLoginEnabled()
      |setLdapLoginDisabled()""".stripMargin,
    "Enable LDAP login.")

  val PROP_KERBEROS_LOGIN: BooleanOption = (
    "spark.ext.h2o.kerberos.login",
    false,
    """setKerberosLoginEnabled()
      |setKerberosLoginDisabled()""".stripMargin,
    "Enable Kerberos login.")

  val PROP_LOGIN_CONF: OptionOption =
    ("spark.ext.h2o.login.conf", None, "setLoginConf(String)", "Login configuration file.")

  val PROP_USER_NAME: OptionOption = (
    "spark.ext.h2o.user.name",
    None,
    "setUserName(String)",
    "Username used for the backend H2O cluster and to authenticate the client against the backend.")

  val PROP_PASSWORD: OptionOption = (
    "spark.ext.h2o.password",
    None,
    "setPassword(String)",
    "Password used to authenticate the client against the backend.")

  val PROP_SSL_CONF: OptionOption = (
    "spark.ext.h2o.internal_security_conf",
    None,
    "setSslConf(String)",
    "Path to a file containing H2O or Sparkling Water internal security configuration.")

  val PROP_AUTO_SSL_FLOW: BooleanOption = (
    "spark.ext.h2o.auto.flow.ssl",
    false,
    """setAutoFlowSslEnabled()
      |setAutoFlowSslDisabled()""".stripMargin,
    "Automatically generate the required key store and password to secure H2O flow by SSL.")

  val PROP_LOG_LEVEL: StringOption = ("spark.ext.h2o.log.level", "INFO", "setLogLevel(String)", "H2O log level.")

  val PROP_LOG_DIR: OptionOption = (
    "spark.ext.h2o.log.dir",
    None,
    "setLogDir(String)",
    "Location of H2O logs. When not specified, it uses {user.dir}/h2ologs/{AppId} or YARN container dir")

  val PROP_BACKEND_HEARTBEAT_INTERVAL: IntOption = (
    "spark.ext.h2o.backend.heartbeat.interval",
    10000,
    "setBackendHeartbeatInterval(Integer)",
    "Interval (in msec) for getting heartbeat from the H2O backend.")

  val PROP_CLOUD_TIMEOUT: IntOption =
    ("spark.ext.h2o.cloud.timeout", 60 * 1000, "setCloudTimeout(Integer)", "Timeout (in msec) for cluster formation.")

  val PROP_NODE_NETWORK_MASK: OptionOption = (
    "spark.ext.h2o.node.network.mask",
    None,
    "setNodeNetworkMask(String)",
    """Subnet selector for H2O running inside park executors. This disables using IP reported by Spark but tries to
      |find IP based on the specified mask.""".stripMargin)

  val PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL: IntOption = (
    "spark.ext.h2o.stacktrace.collector.interval",
    -1,
    "setStacktraceCollectorInterval(Integer)",
    """Interval specifying how often stack traces are taken on each H2O node. -1 means
      |that no stack traces will be taken""".stripMargin)

  val PROP_CONTEXT_PATH: OptionOption =
    ("spark.ext.h2o.context.path", None, "setContextPath(String)", "Context path to expose H2O web server.")

  val PROP_FLOW_SCALA_CELL_ASYNC: BooleanOption = (
    "spark.ext.h2o.flow.scala.cell.async",
    false,
    """setFlowScalaCellAsyncEnabled()
      |setFlowScalaCellAsyncDisabled()""".stripMargin,
    "Decide whether the Scala cells in H2O Flow will run synchronously or Asynchronously. Default is synchronously.")

  val PROP_FLOW_SCALA_CELL_MAX_PARALLEL: IntOption = (
    "spark.ext.h2o.flow.scala.cell.max.parallel",
    -1,
    "setMaxParallelScalaCellJobs(Integer)",
    "Number of max parallel Scala cell jobs. The value -1 means not limited.")

  val PROP_INTERNAL_PORT_OFFSET: IntOption = (
    "spark.ext.h2o.internal.port.offset",
    1,
    "setInternalPortOffset(Integer)",
    """Offset between the API(=web) port and the internal communication port on the client
      |node; ``api_port + port_offset = h2o_port``""".stripMargin)

  val PROP_BASE_PORT: IntOption =
    ("spark.ext.h2o.base.port", 54321, "setBasePort(Integer)", "Base port used for individual H2O nodes")

  val PROP_MOJO_DESTROY_TIMEOUT: IntOption = (
    "spark.ext.h2o.mojo.destroy.timeout",
    10 * 60 * 1000,
    "setMojoDestroyTimeout(Integer)",
    """If a scoring MOJO instance is not used within a Spark executor JVM for a given timeout in milliseconds, it's
      |evicted from executor's cache. Default timeout value is 10 minutes.""".stripMargin)

  val PROP_EXTRA_PROPERTIES: OptionOption = (
    "spark.ext.h2o.extra.properties",
    None,
    "setExtraProperties(String)",
    """A string containing extra parameters passed to H2O nodes during startup. This parameter should be
      |configured only if H2O parameters do not have any corresponding parameters in Sparkling Water.""".stripMargin)

  val PROP_FLOW_DIR: OptionOption =
    ("spark.ext.h2o.flow.dir", None, "setFlowDir(String)", "Directory where flows from H2O Flow are saved.")

  val PROP_FLOW_EXTRA_HTTP_HEADERS: OptionOption = (
    "spark.ext.h2o.flow.extra.http.headers",
    None,
    """setFlowExtraHttpHeaders(Map[String,String])
    |setFlowExtraHttpHeaders(String)""".stripMargin,
    """Extra HTTP headers that will be used in communication between the front-end and back-end part of Flow UI. The
      |headers should be delimited by a new line. Don't forget to escape special characters when passing
      |the parameter from a command line.""".stripMargin)

  val PROP_INTERNAL_SECURE_CONNECTIONS: BooleanOption = (
    "spark.ext.h2o.internal_secure_connections",
    false,
    """setInternalSecureConnectionsEnabled()
      |setInternalSecureConnectionsDisabled()""".stripMargin,
    """Enables secure communications among H2O nodes. The security is based on
      |automatically generated keystore and truststore. This is equivalent for
      |``-internal_secure_conections`` option in H2O Hadoop. More information
      |is available in the H2O documentation.""".stripMargin)

  val PROP_ALLOW_INSECURE_XGBOOST: BooleanOption = (
    "spark.ext.h2o.allow_insecure_xgboost",
    false,
    """setInsecureXGBoostAllowed()
      |setInsecureXGBoostDenied()""".stripMargin,
    """If the property set to true, insecure communication among H2O nodes is
      |allowed for the XGBoost algorithm even if the other security options are enabled""".stripMargin)

  val PROP_CLIENT_IP: OptionOption =
    ("spark.ext.h2o.client.ip", None, "setClientIp(String)", "IP of H2O client node. ")

  val PROP_CLIENT_WEB_PORT: IntOption = (
    "spark.ext.h2o.client.web.port",
    -1,
    "setClientWebPort(Integer)",
    """Exact client port to access web UI. The value ``-1`` means automatic
      |search for a free port starting at ``spark.ext.h2o.base.port``.""".stripMargin)

  val PROP_CLIENT_VERBOSE: BooleanOption = (
    "spark.ext.h2o.client.verbose",
    false,
    """setClientVerboseEnabled()
      |setClientVerboseDisabled()""".stripMargin,
    """The client outputs verbose log output directly into console. Enabling the
      |flag increases the client log level to ``INFO``.""".stripMargin)

  val PROP_CLIENT_NETWORK_MASK: OptionOption = (
    "spark.ext.h2o.client.network.mask",
    None,
    "setClientNetworkMask(String)",
    """Subnet selector for H2O client, this disables using IP reported by Spark
      |but tries to find IP based on the specified mask.""".stripMargin)

  val PROP_CLIENT_FLOW_BASEURL_OVERRIDE: OptionOption = (
    "spark.ext.h2o.client.flow.baseurl.override",
    None,
    "setClientFlowBaseurlOverride(String)",
    """Allows to override the base URL address of Flow UI, including the
      |scheme, which is showed to the user.""".stripMargin)

  val PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT: IntOption = (
    "spark.ext.h2o.cluster.client.retry.timeout",
    PROP_BACKEND_HEARTBEAT_INTERVAL._2 * 6,
    "setClientCheckRetryTimeout(Integer)",
    """Timeout in milliseconds specifying how often we check whether the
      |the client is still connected.""".stripMargin)

  val PROP_VERIFY_SSL_CERTIFICATES: BooleanOption = (
    "spark.ext.h2o.verify_ssl_certificates",
    true,
    "setVerifySslCertificates(Boolean)",
    "Whether certificates should be verified before using in H2O or not.")

  val PROP_KERBERIZED_HIVE_ENABLED: BooleanOption = (
    "spark.ext.h2o.kerberized.hive.enabled",
    false,
    """setKerberizedHiveEnabled()
      |setKerberizedHiveDisabled()""".stripMargin,
    """If enabled, H2O instances will create  JDBC connections to a Kerberized Hive
      |so that all clients can read data from HiveServer2. Don't forget to put
      |a jar with Hive driver on Spark classpath if the internal backend is used.""".stripMargin)

  val PROP_HIVE_HOST: OptionOption = (
    "spark.ext.h2o.hive.host",
    None,
    "setHiveHost(String)",
    "The full address of HiveServer2, for example hostname:10000.")

  val PROP_HIVE_PRINCIPAL: OptionOption = (
    "spark.ext.h2o.hive.principal",
    None,
    "setHivePrincipal(String)",
    "Hiveserver2 Kerberos principal, for example hive/hostname@DOMAIN.COM")

  val PROP_HIVE_JDBC_URL_PATTERN: OptionOption = (
    "spark.ext.h2o.hive.jdbc_url_pattern",
    None,
    "setHiveJdbcUrlPattern(String)",
    "A pattern of JDBC URL used for connecting to Hiveserver2. Example: ``jdbc:hive2://{{host}}/;{{auth}}``")

  val PROP_HIVE_TOKEN: OptionOption =
    ("spark.ext.h2o.hive.token", None, "setHiveToken(String)", "An authorization token to Hive.")

  val PROP_ICED_DIR: OptionOption =
    ("spark.ext.h2o.iced.dir", None, "setIcedDir(String)", "Location of iced directory for H2O nodes.")

  val PROP_REST_API_TIMEOUT: IntOption = (
    "spark.ext.h2o.rest.api.timeout",
    5 * 60 * 1000,
    "setSessionTimeout(Boolean)",
    "Timeout in milliseconds for Rest API requests.")

  /** Language of the connected client. */
  private[sparkling] val PROP_CLIENT_LANGUAGE: (String, String) = ("spark.ext.h2o.client.language", "scala")
}
