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

import org.apache.spark.SparkFiles
import org.apache.spark.h2o.H2OConf
import org.apache.spark.sql.SparkSession
import collection.JavaConverters._

/**
  * Shared configuration independent on used backend
  */
trait SharedBackendConf {
  self: H2OConf =>

  import SharedBackendConf._

  /** Getters */

  /** Generic parameters */
  def backendClusterMode = sparkConf.get(PROP_BACKEND_CLUSTER_MODE._1, PROP_BACKEND_CLUSTER_MODE._2)
  def cloudName = sparkConf.getOption(PROP_CLOUD_NAME._1)
  def nthreads = sparkConf.getInt(PROP_NTHREADS._1, PROP_NTHREADS._2)
  def isH2OReplEnabled = sparkConf.getBoolean(PROP_REPL_ENABLED._1, PROP_REPL_ENABLED._2)
  def scalaIntDefaultNum = sparkConf.getInt(PROP_SCALA_INT_DEFAULT_NUM._1, PROP_SCALA_INT_DEFAULT_NUM._2)
  def isClusterTopologyListenerEnabled = sparkConf.getBoolean(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._2)
  def isSparkVersionCheckEnabled = sparkConf.getBoolean(PROP_SPARK_VERSION_CHECK_ENABLED._1, PROP_SPARK_VERSION_CHECK_ENABLED._2)
  def isFailOnUnsupportedSparkParamEnabled = sparkConf.getBoolean(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._2)
  def jks = sparkConf.getOption(PROP_JKS._1)
  def jksPass = sparkConf.getOption(PROP_JKS_PASS._1)
  def jksAlias = sparkConf.getOption(PROP_JKS_ALIAS._1)
  def hashLogin = sparkConf.getBoolean(PROP_HASH_LOGIN._1, PROP_HASH_LOGIN._2)
  def ldapLogin = sparkConf.getBoolean(PROP_LDAP_LOGIN._1, PROP_LDAP_LOGIN._2)
  def kerberosLogin = sparkConf.getBoolean(PROP_KERBEROS_LOGIN._1, PROP_KERBEROS_LOGIN._2)
  def loginConf = sparkConf.getOption(PROP_LOGIN_CONF._1)
  def userName = sparkConf.getOption(PROP_USER_NAME._1)
  def sslConf = sparkConf.getOption(PROP_SSL_CONF._1)
  def autoFlowSsl = sparkConf.getBoolean(PROP_AUTO_SSL_FLOW._1, PROP_AUTO_SSL_FLOW._2)
  def h2oNodeLogLevel = sparkConf.get(PROP_NODE_LOG_LEVEL._1, PROP_NODE_LOG_LEVEL._2)
  def h2oNodeLogDir  = sparkConf.getOption(PROP_NODE_LOG_DIR._1)
  def uiUpdateInterval = sparkConf.getInt(PROP_UI_UPDATE_INTERVAL._1, PROP_UI_UPDATE_INTERVAL._2)
  def cloudTimeout = sparkConf.getInt(PROP_CLOUD_TIMEOUT._1, PROP_CLOUD_TIMEOUT._2)
  def h2oNodeWebEnabled = sparkConf.getBoolean(PROP_NODE_ENABLE_WEB._1, PROP_NODE_ENABLE_WEB._2)
  def nodeNetworkMask = sparkConf.getOption(PROP_NODE_NETWORK_MASK._1)
  def stacktraceCollectorInterval = sparkConf.getInt(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._2)
  def contextPath = sparkConf.getOption(PROP_CONTEXT_PATH._1)
  def flowScalaCellAsync = sparkConf.getBoolean(PROP_FLOW_SCALA_CELL_ASYNC._1, PROP_FLOW_SCALA_CELL_ASYNC._2)
  def maxParallelScalaCellJobs = sparkConf.getInt(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, PROP_FLOW_SCALA_CELL_MAX_PARALLEL._2)
  def internalPortOffset = sparkConf.getInt(PROP_INTERNAL_PORT_OFFSET._1, PROP_INTERNAL_PORT_OFFSET._2)
  def mojoDestroyTimeout = sparkConf.getInt(PROP_MOJO_DESTROY_TIMEOUT._1, PROP_MOJO_DESTROY_TIMEOUT._2)

  /** H2O Client parameters */
  def flowDir = sparkConf.getOption(PROP_FLOW_DIR._1)
  def flowExtraHttpHeaders = sparkConf.getOption(PROP_FLOW_EXTRA_HTTP_HEADERS._1)
  def clientIp      = sparkConf.getOption(PROP_CLIENT_IP._1)
  def clientIcedDir = sparkConf.getOption(PROP_CLIENT_ICED_DIR._1)
  def h2oClientLogLevel = sparkConf.get(PROP_CLIENT_LOG_LEVEL._1, PROP_CLIENT_LOG_LEVEL._2)
  def h2oClientLogDir = sparkConf.getOption(PROP_CLIENT_LOG_DIR._1)
  def clientBasePort = sparkConf.getInt(PROP_CLIENT_PORT_BASE._1, PROP_CLIENT_PORT_BASE._2)
  def clientWebPort = sparkConf.getInt(PROP_CLIENT_WEB_PORT._1, PROP_CLIENT_WEB_PORT._2)
  def clientVerboseOutput = sparkConf.getBoolean(PROP_CLIENT_VERBOSE._1, PROP_CLIENT_VERBOSE._2)
  def clientNetworkMask = sparkConf.getOption(PROP_CLIENT_NETWORK_MASK._1)
  def ignoreSparkPublicDNS = sparkConf.getBoolean(PROP_CLIENT_IGNORE_SPARK_PUBLIC_DNS._1, PROP_CLIENT_IGNORE_SPARK_PUBLIC_DNS._2)
  def clientWebEnabled = sparkConf.getBoolean(PROP_CLIENT_ENABLE_WEB._1, PROP_CLIENT_ENABLE_WEB._2)
  def clientFlowBaseurlOverride = sparkConf.getOption(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1)

  def runsInExternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_EXTERNAL
  def runsInInternalClusterMode: Boolean = backendClusterMode.toLowerCase() == BACKEND_MODE_INTERNAL

  def clientCheckRetryTimeout = sparkConf.getInt(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._2)

  /** Setters */

  /** Generic parameters */
  def setInternalClusterMode() = {
    if (runsInExternalClusterMode){
      logWarning("Using internal cluster mode!")
    }
    setBackendClusterMode(BACKEND_MODE_INTERNAL)
  }

  def setExternalClusterMode() = {
    if (runsInInternalClusterMode){
      logWarning("Using external cluster mode!")
    }
    setBackendClusterMode(BACKEND_MODE_EXTERNAL)
  }

  def setCloudName(cloudName: String) = set(PROP_CLOUD_NAME._1, cloudName)
  def setNthreads(numThreads: Int) = set(PROP_NTHREADS._1, nthreads.toString)

  def setReplEnabled() = set(PROP_REPL_ENABLED._1, true)
  def setReplDisabled() = set(PROP_REPL_ENABLED._1, false)

  def setDefaultNumReplSessions(numSessions: Int) = set(PROP_SCALA_INT_DEFAULT_NUM._1, numSessions.toString)

  def setClusterTopologyListenerEnabled() = set(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, true)
  def setClusterTopologyListenerDisabled() = set(PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED._1, false)

  def setSparkVersionCheckEnable() = set(PROP_SPARK_VERSION_CHECK_ENABLED._1, true)
  def setSparkVersionCheckDisabled() = set(PROP_SPARK_VERSION_CHECK_ENABLED._1, false)

  def setFailOnUnsupportedSparkParamEnabled() = set(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, true)
  def setFailOnUnsupportedSparkParamDisabled() = set(PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM._1, false)

  def setJks(path: String) = set(PROP_JKS._1, path)
  def setJksPass(password: String) = set(PROP_JKS_PASS._1, password)
  def setJksAlias(alias: String) = set(PROP_JKS_ALIAS._1, alias)

  def setHashLoginEnabled() = set(PROP_HASH_LOGIN._1, true)
  def setHashLoginDisabled() = set(PROP_HASH_LOGIN._1, false)

  def setLdapLoginEnabled() = set(PROP_LDAP_LOGIN._1, true)
  def setLdapLoginDisabled() = set(PROP_LDAP_LOGIN._1, false)

  def setKerberosLoginEnabled() = set(PROP_KERBEROS_LOGIN._1, true)
  def setKerberosLoginDisabled() = set(PROP_KERBEROS_LOGIN._1, false)

  def setLoginConf(file: String) = {
    SparkSession.builder().getOrCreate().sparkContext.addFile(file)
    set(PROP_LOGIN_CONF._1, SparkFiles.get(new File(file).getName))
  }
  def setUserName(username: String) = set(PROP_USER_NAME._1, username)
  def setSslConf(path: String) = set(PROP_SSL_CONF._1, path)

  def setAutoFlowSslEnabled() = set(PROP_AUTO_SSL_FLOW._1, true)
  def setAutoFlowSslDisabled() = set(PROP_AUTO_SSL_FLOW._1, false)

  def setH2ONodeLogLevel(level: String) = set(PROP_NODE_LOG_LEVEL._1, level)
  def setH2ONodeLogDir(dir: String) = set(PROP_NODE_LOG_DIR._1, dir)
  def setUiUpdateInterval(interval: Int) = set(PROP_UI_UPDATE_INTERVAL._1, interval.toString)
  def setCloudTimeout(timeout: Int) = set(PROP_CLOUD_TIMEOUT._1, timeout.toString)

  def setH2ONodeWebEnabled() = set(PROP_NODE_ENABLE_WEB._1, true)
  def setH2ONodeWebDisabled() = set(PROP_NODE_ENABLE_WEB._1, false)

  def setNodeNetworkMask(mask: String) = set(PROP_NODE_NETWORK_MASK._1, mask)

  def setStacktraceCollectorInterval(interval: Int) = set(PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL._1, interval.toString)
  def setContextPath(contextPath: String) = set(PROP_CONTEXT_PATH._1, contextPath)

  def setFlowScalaCellAsyncEnabled() = set(PROP_FLOW_SCALA_CELL_ASYNC._1, true)
  def setFlowScalaCellAsyncDisabled() = set(PROP_FLOW_SCALA_CELL_ASYNC._1, false)

  def setMaxParallelScalaCellJobs(limit: Int) = set(PROP_FLOW_SCALA_CELL_MAX_PARALLEL._1, limit.toString)
  def setInternalPortOffset(offset: Int) = set(PROP_INTERNAL_PORT_OFFSET._1, offset.toString)

  def setMojoDestroyTimeout(timeoutInMilliseconds: Int): H2OConf = set(PROP_MOJO_DESTROY_TIMEOUT._1, timeoutInMilliseconds.toString)

  /** H2O Client parameters */
  def setFlowDir(dir: String) = set(PROP_FLOW_DIR._1, dir)
  def setFlowExtraHttpHeaders(headers: java.util.HashMap[String, String]): H2OConf = { // Py4J mapping
    setFlowExtraHttpHeaders(headers.asScala.toMap[String, String])
  }
  def setFlowExtraHttpHeaders(headers: Map[String, String]): H2OConf = {
    val stringRepresentation =  headers.map(header => s"${header._1}: ${header._2}").mkString("\n")
    set(PROP_FLOW_EXTRA_HTTP_HEADERS._1, stringRepresentation)
  }
  def setClientIp(ip: String) = set(PROP_CLIENT_IP._1, ip)
  def setClientIcedDir(icedDir: String) = set(PROP_CLIENT_ICED_DIR._1, icedDir)
  def setH2OClientLogLevel(level: String) = set(PROP_CLIENT_LOG_LEVEL._1, level)
  def setH2OClientLogDir(dir: String) = set(PROP_CLIENT_LOG_DIR._1, dir)
  def setClientPortBase(basePort: Int) = set(PROP_CLIENT_PORT_BASE._1, basePort.toString)
  def setClientWebPort(port: Int) = set(PROP_CLIENT_WEB_PORT._1, port.toString)

  def setClientVerboseEnabled() = set(PROP_CLIENT_VERBOSE._1, true)
  def setClientVerboseDisabled() = set(PROP_CLIENT_VERBOSE._1, false)

  def setClientNetworkMask(mask: String) = set(PROP_CLIENT_NETWORK_MASK._1, mask)

  def setIgnoreSparkPublicDNSEnabled() = set(PROP_CLIENT_IGNORE_SPARK_PUBLIC_DNS._1, true)
  def setIgnoreSparkPublicDNSDisabled() = set(PROP_CLIENT_IGNORE_SPARK_PUBLIC_DNS._1, false)

  def setClientWebEnabled() = set(PROP_CLIENT_ENABLE_WEB._1, true)
  def setClientWebDisabled() = set(PROP_CLIENT_ENABLE_WEB._1, false)

  def setClientFlowBaseurlOverride(baseUrl: String) = set(PROP_CLIENT_FLOW_BASEURL_OVERRIDE._1, baseUrl)

  private[this] def setBackendClusterMode(backendClusterMode: String) = {
    set(PROP_BACKEND_CLUSTER_MODE._1, backendClusterMode)
  }

  def setClientCheckRetryTimeout(timeout: Int) = set(PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT._1, timeout.toString)

}

object SharedBackendConf {

  val BACKEND_MODE_INTERNAL = "internal"
  val BACKEND_MODE_EXTERNAL = "external"

  /**
    * This option can be set either to "internal" or "external"
    * When set to "external" H2O Context is created by connecting to existing H2O cluster, otherwise it creates
    * H2O cluster living in Spark - that means that each Spark executor will have one h2o instance running in it.
    * The internal is not recommended for big clusters and clusters where Spark executors are not stable.
    */
  val PROP_BACKEND_CLUSTER_MODE = ("spark.ext.h2o.backend.cluster.mode", BACKEND_MODE_INTERNAL)

  /** Configuration property - name of H2O cloud */
  val PROP_CLOUD_NAME = ("spark.ext.h2o.cloud.name", None)

  /** Limit for number of threads used by H2O, default -1 means unlimited */
  val PROP_NTHREADS = ("spark.ext.h2o.nthreads", -1)

  /** Enable/Disable Sparkling-Water REPL **/
  val PROP_REPL_ENABLED = ("spark.ext.h2o.repl.enabled", true)

  /** Number of executors started at the start of h2o services, by default 1 */
  val PROP_SCALA_INT_DEFAULT_NUM = ("spark.ext.scala.int.default.num", 1)

  /** Enable/Disable listener which kills H2O when there is a change in underlying cluster's topology**/
  val PROP_CLUSTER_TOPOLOGY_LISTENER_ENABLED = ("spark.ext.h2o.topology.change.listener.enabled", true)

  /** Enable/Disable check for Spark version. */
  val PROP_SPARK_VERSION_CHECK_ENABLED = ("spark.ext.h2o.spark.version.check.enabled", true)

  /** Enable/Disable exit on unsupported Spark parameters. */
  val PROP_FAIL_ON_UNSUPPORTED_SPARK_PARAM = ("spark.ext.h2o.fail.on.unsupported.spark.param", true)

  /** Path to Java KeyStore file. */
  val PROP_JKS = ("spark.ext.h2o.jks", None)

  /** Password for Java KeyStore file. */
  val PROP_JKS_PASS = ("spark.ext.h2o.jks.pass", None)

  /** Alias for certificate in keystore to secure Flow */
  val PROP_JKS_ALIAS = ("spark.ext.h2o.jks.alias", None)

  /** Enable hash login. */
  val PROP_HASH_LOGIN = ("spark.ext.h2o.hash.login", false)

  /** Enable LDAP login. */
  val PROP_LDAP_LOGIN = ("spark.ext.h2o.ldap.login", false)

  /** Enable Kerberos login. */
  val PROP_KERBEROS_LOGIN = ("spark.ext.h2o.kerberos.login", false)

  /** Login configuration file. */
  val PROP_LOGIN_CONF = ("spark.ext.h2o.login.conf", None)

  /** Override user name for cluster. */
  val PROP_USER_NAME = ("spark.ext.h2o.user.name", None)

  /** Path to Java KeyStore file used for the internal SSL communication. */
  val PROP_SSL_CONF = ("spark.ext.h2o.internal_security_conf", None)

  /** Automatically generate key store for H2O Flow SSL */
  val PROP_AUTO_SSL_FLOW = ("spark.ext.h2o.auto.flow.ssl", false)

  /** H2O internal log level for launched remote nodes. */
  val PROP_NODE_LOG_LEVEL = ("spark.ext.h2o.node.log.level", "INFO")

  /** Location of log directory for remote nodes. */
  val PROP_NODE_LOG_DIR = ("spark.ext.h2o.node.log.dir", None)

  /** Interval for updates of Spark UI in milliseconds */
  val PROP_UI_UPDATE_INTERVAL = ("spark.ext.h2o.ui.update.interval", 10000)

  /** Configuration property - timeout for cloud up. */
  val PROP_CLOUD_TIMEOUT = ("spark.ext.h2o.cloud.timeout", 60 * 1000)

  /** Enable or disable web on h2o worker nodes. It is disabled by default for security reasons. */
  val PROP_NODE_ENABLE_WEB = ("spark.ext.h2o.node.enable.web", false)

  /** Subnet selector for H2O nodes running inside executors - if the mask is specified then Spark network setup is not discussed. */
  val PROP_NODE_NETWORK_MASK = ("spark.ext.h2o.node.network.mask", None)

  /** Set how often in seconds stack traces are taken on each h2o node. -1 represents that the stack traces are not collected. */
  val PROP_NODE_STACK_TRACE_COLLECTOR_INTERVAL = ("spark.ext.h2o.stacktrace.collector.interval", -1)

  /** H2O's URL context path */
  val PROP_CONTEXT_PATH = ("spark.ext.h2o.context.path", None)

  /** Decide whether Scala cells are running synchronously or asynchronously */
  val PROP_FLOW_SCALA_CELL_ASYNC = ("spark.ext.h2o.flow.scala.cell.async", false)

  /** Number of max parallel Scala cell jobs. */
  val PROP_FLOW_SCALA_CELL_MAX_PARALLEL = ("spark.ext.h2o.flow.scala.cell.max.parallel", -1)

  /** Offset between the API(=web) port and the internal communication port; api_port + port_offset = h2o_port */
  val PROP_INTERNAL_PORT_OFFSET = ("spark.ext.h2o.internal.port.offset", 1)

  /**
    * If a scoring MOJO instance is not used within a Spark executor JVM for a given timeout in milliseconds,
    * it's evicted from executor's cache.
    */
  val PROP_MOJO_DESTROY_TIMEOUT = ("spark.ext.h2o.mojo.destroy.timeout", 10 * 60 * 1000)

  /** Path to flow dir. */
  val PROP_FLOW_DIR = ("spark.ext.h2o.client.flow.dir", None)

  /** Extra http headers for Flow UI */
  val PROP_FLOW_EXTRA_HTTP_HEADERS = ("spark.ext.h2o.client.flow.extra.http.headers", None)

  /** IP of H2O client node */
  val PROP_CLIENT_IP = ("spark.ext.h2o.client.ip", None)

  /** Location of iced directory for the driver instance. */
  val PROP_CLIENT_ICED_DIR = ("spark.ext.h2o.client.iced.dir", None)

  /** H2O log level for client running in Spark driver */
  val PROP_CLIENT_LOG_LEVEL = ("spark.ext.h2o.client.log.level", "INFO")

  /** Location of log directory for the driver instance. */
  val PROP_CLIENT_LOG_DIR = ("spark.ext.h2o.client.log.dir", None)

  /** Port on which H2O client publishes its API. If already occupied, the next odd port is tried and so on */
  val PROP_CLIENT_PORT_BASE = ("spark.ext.h2o.client.port.base", 54321)

  /** Exact client port to access web UI.
    * The value `-1` means automatic search for free port starting at `spark.ext.h2o.port.base`. */
  val PROP_CLIENT_WEB_PORT = ("spark.ext.h2o.client.web.port", -1)

  /** Print detailed messages to client stdout */
  val PROP_CLIENT_VERBOSE = ("spark.ext.h2o.client.verbose", false)

  /** Subnet selector for H2O client - if the mask is specified then Spark network setup is not discussed. */
  val PROP_CLIENT_NETWORK_MASK = ("spark.ext.h2o.client.network.mask", None)

  /** Ignore SPARK_PUBLIC_DNS setting on the H2O client. The option still applies to the Spark application. */
  val PROP_CLIENT_IGNORE_SPARK_PUBLIC_DNS = ("spark.ext.h2o.client.ignore.SPARK_PUBLIC_DNS", false)

  /** Enable or disable web on h2o client node. It is enabled by default. Disabling the web just
    * on the client node just restricts everybody from accessing flow, the internal ports
    * between client and rest of the cluster remain open
    */
  val PROP_CLIENT_ENABLE_WEB = ("spark.ext.h2o.client.enable.web", true)

  /**
    * Allows to override the base URL address of Flow UI, including the scheme, which is showed
    * to the user.
    */
  val PROP_CLIENT_FLOW_BASEURL_OVERRIDE = ("spark.ext.h2o.client.flow.baseurl.override", None)

  /** Timeout in milliseconds specifying how often we check whether the client is still connected */
  val PROP_EXTERNAL_CLIENT_RETRY_TIMEOUT = ("spark.ext.h2o.cluster.client.retry.timeout", 60000)

}
