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

import org.apache.spark.h2o.H2OConf

import scala.collection.JavaConverters._

/**
 * Shared configuration independent on used backend
 */
trait SharedBackendConf {
  self: H2OConf =>

  import SharedBackendConf._

  /** Getters */

  /** Generic parameters */
  def backendClusterMode: String = sparkConf.get(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2)

  def cloudName: Option[String] = sparkConf.getOption(PROP_CLOUD_NAME._1)

  def nthreads: Int = sparkConf.getInt(PROP_NTHREADS._1, PROP_NTHREADS._2)

  def isH2OReplEnabled: Boolean = sparkConf.getBoolean(PROP_REPL_ENABLED._1, PROP_REPL_ENABLED._2)

  def scalaIntDefaultNum: Int = sparkConf.getInt(PROP_SCALA_INT_DEFAULT_NUM._1, PROP_SCALA_INT_DEFAULT_NUM._2)

  def isClusterTopologyListenerEnabled: Boolean = sparkConf.getBoolean(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._2)

  def isSparkVersionCheckEnabled: Boolean = sparkConf.getBoolean(PROP_SPARK_VERSION_CHECK_ENABLED._1, PROP_SPARK_VERSION_CHECK_ENABLED._2)

  def isFailOnUnsupportedSparkParamEnabled: Boolean = sparkConf.getBoolean(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._2)

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

  def h2oNodeLogLevel: String = sparkConf.get(PROP_NODE_LOG_LEVEL._1, PROP_NODE_LOG_LEVEL._2)

  def h2oNodeLogDir: Option[String] = sparkConf.getOption(PROP_NODE_LOG_DIR._1)

  def backendHeartbeatInterval: Int = sparkConf.getInt(PROP_BACKEND_HEARTBEAT_INTERVAL._1, PROP_BACKEND_HEARTBEAT_INTERVAL._2)

  def cloudTimeout: Int = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)

  def nodeNetworkMask: Option[String] = sparkConf.getOption(PROP_NODE_NETWORK_MASK._1)

  def stacktraceCollectorInterval: Int = sparkConf.getInt(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._2)

  def contextPath: Option[String] = sparkConf.getOption(PROP_CONTEXT_PATH._1)

  def flowScalaCellAsync: Boolean = sparkConf.getBoolean(PROP_FLOW_SCALA_CELL_ASYNC._1, PROP_FLOW_SCALA_CELL_ASYNC._2)

  def maxParallelScalaCellJobs: Int = sparkConf.getInt(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, PROP_FLOW_SCALA_CELL_MAX_PARALLEL._2)

  def internalPortOffset: Int = sparkConf.getInt(PROP_INTERNAL_PORT_OFFSET._1, PROP_INTERNAL_PORT_OFFSET._2)

  def nodeBasePort: Int = sparkConf.getInt(PROP_NODE_PORT_BASE._1, PROP_NODE_PORT_BASE._2)

  def mojoDestroyTimeout: Int = sparkConf.getInt(PROP_MOJO_DESTROY_TIMEOUT._1, PROP_MOJO_DESTROY_TIMEOUT._2)

  def nodeExtraProperties: Option[String] = sparkConf.getOption(PROP_NODE_EXTRA_PROPERTIES._1)

  def flowExtraHttpHeaders: Option[String] = sparkConf.getOption(PROP_FLOW_EXTRA_HTTP_HEADERS._1)

  def isInternalSecureConnectionsEnabled: Boolean = sparkConf.getBoolean(PROP_INTERNAL_SECURE_CONNECTIONS._1,
    PROP_INTERNAL_SECURE_CONNECTIONS._2)

  def isSparkTimeZoneFollowed: Boolean = sparkConf.getBoolean(PROP_FOLLOW_SPARK_TIME_ZONE._1, PROP_FOLLOW_SPARK_TIME_ZONE._2)

  /** H2O Client parameters */
  def flowDir: Option[String] = sparkConf.getOption(PROP_FLOW_DIR._1)

  def clientIp: Option[String] = sparkConf.getOption(PROP_CLIENT_IP._1)

  def clientIcedDir: Option[String] = sparkConf.getOption(PROP_CLIENT_ICED_DIR._1)

  def h2oClientLogLevel: String = sparkConf.get(PROP_CLIENT_LOG_LEVEL._1, PROP_CLIENT_LOG_LEVEL._2)

  def h2oClientLogDir: Option[String] = sparkConf.getOption(PROP_CLIENT_LOG_DIR._1)

  def clientBasePort: Int = sparkConf.getInt(PROP_CLIENT_PORT_BASE._1, PROP_CLIENT_PORT_BASE._2)

  def clientWebPort: Int = sparkConf.getInt(PROP_CLIENT_WEB_PORT._1, PROP_CLIENT_WEB_PORT._2)

  def clientVerboseOutput: Boolean = sparkConf.getBoolean(PROP_CLIENT_VERBOSE._1, PROP_CLIENT_VERBOSE._2)

  def clientNetworkMask: Option[String] = sparkConf.getOption(PROP_CLIENT_NETWORK_MASK._1)

  def clientFlowBaseurlOverride: Option[String] = sparkConf.getOption(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1)

  def clientExtraProperties: Option[String] = sparkConf.getOption(PROP_CLIENT_EXTRA_PROPERTIES._1)

  def runsInExternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_EXTERNAL

  def runsInInternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_INTERNAL

  def clientCheckRetryTimeout: Int = sparkConf.getInt(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2)

  def verifySslCertificates: Boolean = sparkConf.getBoolean(PROP_VERIFY_SSL_CERTIFICATES._1, PROP_VERIFY_SSL_CERTIFICATES._2)

  /** Setters */

  /** Generic parameters */
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

  def setH2ONodeLogLevel(level: String): H2OConf = set(PROP_NODE_LOG_LEVEL._1, level)

  def setH2ONodeLogDir(dir: String): H2OConf = set(PROP_NODE_LOG_DIR._1, dir)

  def setBackendHeartbeatInterval(interval: Int): H2OConf = set(PROP_BACKEND_HEARTBEAT_INTERVAL._1, interval.toString)

  def setCloudTimeout(timeout: Int): H2OConf = set(PROP_CLOUD_TIMEOUT._1, timeout.toString)

  def setNodeNetworkMask(mask: String): H2OConf = set(PROP_NODE_NETWORK_MASK._1, mask)

  def setStacktraceCollectorInterval(interval: Int): H2OConf = set(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, interval.toString)

  def setContextPath(contextPath: String): H2OConf = set(PROP_CONTEXT_PATH._1, contextPath)

  def setFlowScalaCellAsyncEnabled(): H2OConf = set(PROP_FLOW_SCALA_CELL_ASYNC._1, value = true)

  def setFlowScalaCellAsyncDisabled(): H2OConf = set(PROP_FLOW_SCALA_CELL_ASYNC._1, value = false)

  def setMaxParallelScalaCellJobs(limit: Int): H2OConf = set(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, limit.toString)

  def setInternalPortOffset(offset: Int): H2OConf = set(PROP_INTERNAL_PORT_OFFSET._1, offset.toString)

  def setNodeBasePort(port: Int): H2OConf = set(PROP_NODE_PORT_BASE._1, port.toString)

  def setMojoDestroyTimeout(timeoutInMilliseconds: Int): H2OConf = set(PROP_MOJO_DESTROY_TIMEOUT._1, timeoutInMilliseconds.toString)

  def setNodeExtraProperties(extraProperties: String): H2OConf = set(PROP_NODE_EXTRA_PROPERTIES._1, extraProperties)

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

  def setSparkTimeZoneFollowingEnabled(): H2OConf = set(PROP_FOLLOW_SPARK_TIME_ZONE._1, value = true)

  def setSparkTimeZoneFollowingDisabled(): H2OConf = set(PROP_FOLLOW_SPARK_TIME_ZONE._1, value = false)

  /** H2O Client parameters */
  def setFlowDir(dir: String): H2OConf = set(PROP_FLOW_DIR._1, dir)

  def setClientIp(ip: String): H2OConf = set(PROP_CLIENT_IP._1, ip)

  def setClientIcedDir(icedDir: String): H2OConf = set(PROP_CLIENT_ICED_DIR._1, icedDir)

  def setH2OClientLogLevel(level: String): H2OConf = set(PROP_CLIENT_LOG_LEVEL._1, level)

  def setH2OClientLogDir(dir: String): H2OConf = set(PROP_CLIENT_LOG_DIR._1, dir)

  def setClientBasePort(basePort: Int): H2OConf = set(PROP_CLIENT_PORT_BASE._1, basePort.toString)

  def setClientWebPort(port: Int): H2OConf = set(PROP_CLIENT_WEB_PORT._1, port.toString)

  def setClientVerboseEnabled(): H2OConf = set(PROP_CLIENT_VERBOSE._1, value = true)

  def setClientVerboseDisabled(): H2OConf = set(PROP_CLIENT_VERBOSE._1, value = false)

  def setClientNetworkMask(mask: String): H2OConf = set(PROP_CLIENT_NETWORK_MASK._1, mask)

  def setClientFlowBaseurlOverride(baseUrl: String): H2OConf = set(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1, baseUrl)

  private[this] def setBackendClusterMode(backendClusterMode: String) = {
    set(PROP_BACKEND_CLUSTER_MODE._1, backendClusterMode)
  }

  def setClientCheckRetryTimeout(timeout: Int): H2OConf = set(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, timeout.toString)

  def setClientExtraProperties(extraProperties: String): H2OConf = set(PROP_CLIENT_EXTRA_PROPERTIES._1, extraProperties)

  def setVerifySslCertificates(verify: Boolean): H2OConf = set(PROP_VERIFY_SSL_CERTIFICATES._1, verify)

  private[backend] def getFileProperties: Seq[(String, _)] = Seq(PROP_JKS, PROP_LOGIN_CONF, PROP_SSL_CONF)
}

object SharedBackendConf {

  val BACKEND_MODE_INTERNAL: String = "internal"
  val BACKEND_MODE_EXTERNAL: String = "external"

  /**
   * This option can be set either to "internal" or "external"
   * When set to "external" H2O Context is created by connecting to existing H2O cluster, otherwise it creates
   * H2O cluster living in Spark - that means that each Spark executor will have one h2o instance running in it.
   * The internal is not recommended for big clusters and clusters where Spark executors are not stable.
   */
  val PROP_BACKEND_CLUSTER_MODE: (String, String) = ("spark.ext.h2o.backend.cluster.mode", BACKEND_MODE_INTERNAL)

  /** Configuration property - name of H2O cloud */
  val PROP_CLOUD_NAME: (String, None.type) = ("spark.ext.h2o.cloud.name", None)

  /** Limit for number of threads used by H2O, default -1 means unlimited */
  val PROP_NTHREADS: (String, Int) = ("spark.ext.h2o.nthreads", -1)

  /** Enable/Disable Sparkling-Water REPL **/
  val PROP_REPL_ENABLED: (String, Boolean) = ("spark.ext.h2o.repl.enabled", true)

  /** Number of executors started at the start of h2o services, by default 1 */
  val PROP_SCALA_INT_DEFAULT_NUM: (String, Int) = ("spark.ext.scala.int.default.num", 1)

  /** Enable/Disable listener which kills H2O when there is a change in underlying cluster's topology **/
  val PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED: (String, Boolean) = ("spark.ext.h2o.topology.change.listener.enabled", true)

  /** Enable/Disable check for Spark version. */
  val PROP_SPARK_VERSION_CHECK_ENABLED: (String, Boolean) = ("spark.ext.h2o.spark.version.check.enabled", true)

  /** Enable/Disable exit on unsupported Spark parameters. */
  val PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM: (String, Boolean) = ("spark.ext.h2o.fail.on.unsupported.spark.param", true)

  /** Path to Java KeyStore file. */
  val PROP_JKS: (String, None.type) = ("spark.ext.h2o.jks", None)

  /** Password for Java KeyStore file. */
  val PROP_JKS_PASS: (String, None.type) = ("spark.ext.h2o.jks.pass", None)

  /** Alias for certificate in keystore to secure Flow */
  val PROP_JKS_ALIAS: (String, None.type) = ("spark.ext.h2o.jks.alias", None)

  /** Enable hash login. */
  val PROP_HASH_LOGIN: (String, Boolean) = ("spark.ext.h2o.hash.login", false)

  /** Enable LDAP login. */
  val PROP_LDAP_LOGIN: (String, Boolean) = ("spark.ext.h2o.ldap.login", false)

  /** Enable Kerberos login. */
  val PROP_KERBEROS_LOGIN: (String, Boolean) = ("spark.ext.h2o.kerberos.login", false)

  /** Login configuration file. */
  val PROP_LOGIN_CONF: (String, None.type) = ("spark.ext.h2o.login.conf", None)

  /** User name for cluster and the client authentication. */
  val PROP_USER_NAME: (String, None.type) = ("spark.ext.h2o.user.name", None)

  /** Password for the client authentication. */
  val PROP_PASSWORD: (String, None.type) = ("spark.ext.h2o.password", None)

  /** Path to Java KeyStore file used for the internal SSL communication. */
  val PROP_SSL_CONF: (String, None.type) = ("spark.ext.h2o.internal_security_conf", None)

  /** Automatically generate key store for H2O Flow SSL */
  val PROP_AUTO_SSL_FLOW: (String, Boolean) = ("spark.ext.h2o.auto.flow.ssl", false)

  /** H2O internal log level for launched remote nodes. */
  val PROP_NODE_LOG_LEVEL: (String, String) = ("spark.ext.h2o.node.log.level", "INFO")

  /** Location of log directory for remote nodes. */
  val PROP_NODE_LOG_DIR: (String, None.type) = ("spark.ext.h2o.node.log.dir", None)

  /** Interval used to ping and check the H2O backend status. */
  val PROP_BACKEND_HEARTBEAT_INTERVAL: (String, Int) = ("spark.ext.h2o.backend.heartbeat.interval", 10000)

  /** Configuration property - timeout for cloud up. */
  val PROP_CLOUD_TIMEOUT: (String, Int) = ("spark.ext.h2o.cloud.timeout", 60 * 1000)

  /** Subnet selector for H2O nodes running inside executors - if the mask is specified then Spark network setup is not discussed. */
  val PROP_NODE_NETWORK_MASK: (String, None.type) = ("spark.ext.h2o.node.network.mask", None)

  /** Set how often in seconds stack traces are taken on each h2o node. -1 represents that the stack traces are not collected. */
  val PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL: (String, Int) = ("spark.ext.h2o.stacktrace.collector.interval", -1)

  /** H2O's URL context path */
  val PROP_CONTEXT_PATH: (String, None.type) = ("spark.ext.h2o.context.path", None)

  /** Decide whether Scala cells are running synchronously or asynchronously */
  val PROP_FLOW_SCALA_CELL_ASYNC: (String, Boolean) = ("spark.ext.h2o.flow.scala.cell.async", false)

  /** Number of max parallel Scala cell jobs. */
  val PROP_FLOW_SCALA_CELL_MAX_PARALLEL: (String, Int) = ("spark.ext.h2o.flow.scala.cell.max.parallel", -1)

  /** Offset between the API(=web) port and the internal communication port; api_port + port_offset = h2o_port */
  val PROP_INTERNAL_PORT_OFFSET: (String, Int) = ("spark.ext.h2o.internal.port.offset", 1)

  /** Configuration property - base port used for individual H2O nodes configuration. */
  val PROP_NODE_PORT_BASE: (String, Int) = ("spark.ext.h2o.node.port.base", 54321)

  /**
   * If a scoring MOJO instance is not used within a Spark executor JVM for a given timeout in milliseconds,
   * it's evicted from executor's cache.
   */
  val PROP_MOJO_DESTROY_TIMEOUT: (String, Int) = ("spark.ext.h2o.mojo.destroy.timeout", 10 * 60 * 1000)

  /** Extra properties passed to H2O nodes during startup. */
  val PROP_NODE_EXTRA_PROPERTIES: (String, None.type) = ("spark.ext.h2o.node.extra", None)

  /** Path to flow dir. */
  val PROP_FLOW_DIR: (String, None.type) = ("spark.ext.h2o.client.flow.dir", None)

  /** Extra http headers for Flow UI */
  val PROP_FLOW_EXTRA_HTTP_HEADERS: (String, None.type) = ("spark.ext.h2o.flow.extra.http.headers", None)

  /** Secure internal connections by automatically generated credentials */
  val PROP_INTERNAL_SECURE_CONNECTIONS: (String, Boolean) = ("spark.ext.h2o.internal_secure_connections", false)

  /** Whether the H2O cluster should follow the time zone settings of Spark ("spark.sql.session.timeZone"). */
  val PROP_FOLLOW_SPARK_TIME_ZONE: (String, Boolean) = ("spark.ext.h2o.follow_spark_time_zone", true)

  /** IP of H2O client node */
  val PROP_CLIENT_IP: (String, None.type) = ("spark.ext.h2o.client.ip", None)

  /** Location of iced directory for the driver instance. */
  val PROP_CLIENT_ICED_DIR: (String, None.type) = ("spark.ext.h2o.client.iced.dir", None)

  /** H2O log level for client running in Spark driver */
  val PROP_CLIENT_LOG_LEVEL: (String, String) = ("spark.ext.h2o.client.log.level", "INFO")

  /** Location of log directory for the driver instance. */
  val PROP_CLIENT_LOG_DIR: (String, None.type) = ("spark.ext.h2o.client.log.dir", None)

  /** Port on which H2O client publishes its API. If already occupied, the next odd port is tried and so on */
  val PROP_CLIENT_PORT_BASE: (String, Int) = ("spark.ext.h2o.client.port.base", 54321)

  /** Exact client port to access web UI.
   * The value `-1` means automatic search for free port starting at `spark.ext.h2o.port.base`. */
  val PROP_CLIENT_WEB_PORT: (String, Int) = ("spark.ext.h2o.client.web.port", -1)

  /** Print detailed messages to client stdout */
  val PROP_CLIENT_VERBOSE: (String, Boolean) = ("spark.ext.h2o.client.verbose", false)

  /** Subnet selector for H2O client - if the mask is specified then Spark network setup is not discussed. */
  val PROP_CLIENT_NETWORK_MASK: (String, None.type) = ("spark.ext.h2o.client.network.mask", None)

  /**
   * Allows to override the base URL address of Flow UI, including the scheme, which is showed
   * to the user.
   */
  val PROP_CLIENT_FLOW_BASEURL_OVERRIDE: (String, None.type) = ("spark.ext.h2o.client.flow.baseurl.override", None)

  /** Timeout in milliseconds specifying how often the H2O backend checks whether the Sparkling Water
   * client (either H2O client or REST) is connected
   */
  val PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT: (String, Int) = ("spark.ext.h2o.cluster.client.retry.timeout", PROP_BACKEND_HEARTBEAT_INTERVAL._2 * 6)

  val PROP_REST_API_BASED_CLIENT: (String, Boolean) = ("spark.ext.h2o.rest.api.based.client", false)

  /** Extra properties passed to H2O client during startup. */
  val PROP_CLIENT_EXTRA_PROPERTIES: (String, None.type) = ("spark.ext.h2o.client.extra", None)

  /** Whether certificates should be verified before using in H2O or not. */
  val PROP_VERIFY_SSL_CERTIFICATES: (String, Boolean) = ("spark.ext.h2o.verify_ssl_certificates", true)
}
