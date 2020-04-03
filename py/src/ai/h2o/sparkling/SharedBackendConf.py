#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import warnings

from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils

class SharedBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def backend_cluster_mode(self):
        warnings.warn("Method 'backend_cluster_mode' is deprecated and will be removed in the next major release. Please use 'backendClusterMode'.")
        return self.backendClusterMode()

    def backendClusterMode(self):
        return self._jconf.backendClusterMode()

    def cloud_name(self):
        warnings.warn("Method 'cloud_name' is deprecated and will be removed in the next major release. Please use 'cloudName'.")
        return self.cloudName()

    def cloudName(self):
        return self._get_option(self._jconf.cloudName())

    def nthreads(self):
        return self._jconf.nthreads()

    def is_h2o_repl_enabled(self):
        warnings.warn("Method 'is_h2o_repl_enabled' is deprecated and will be removed in the next major release. Please use 'isH2OReplEnabled'.")
        return self.isH2OReplEnabled()

    def isH2OReplEnabled(self):
        return self._jconf.isH2OReplEnabled()

    def scala_int_default_num(self):
        warnings.warn("Method 'scala_int_default_num' is deprecated and will be removed in the next major release. Please use 'scalaIntDefaultNum'.")
        return self.scalaIntDefaultNum()

    def scalaIntDefaultNum(self):
        return self._jconf.scalaIntDefaultNum()

    def is_cluster_topology_listener_enabled(self):
        warnings.warn("Method 'is_cluster_topology_listener_enabled' is deprecated and will be removed in the next major release. Please use 'isClusterTopologyListenerEnabled'.")
        return self.isClusterTopologyListenerEnabled()

    def isClusterTopologyListenerEnabled(self):
        return self._jconf.isClusterTopologyListenerEnabled()

    def is_spark_version_check_enabled(self):
        warnings.warn("Method 'is_spark_version_check_enabled' is deprecated and will be removed in the next major release. Please use 'isSparkVersionCheckEnabled'.")
        return self.isSparkVersionCheckEnabled()

    def isSparkVersionCheckEnabled(self):
        return self._jconf.isSparkVersionCheckEnabled()

    def is_fail_on_unsupported_spark_param_enabled(self):
        warnings.warn("Method 'is_fail_on_unsupported_spark_param_enabled' is deprecated and will be removed in the next major release. Please use 'isFailOnUnsupportedSparkParamEnabled'.")
        return self.isFailOnUnsupportedSparkParamEnabled()

    def isFailOnUnsupportedSparkParamEnabled(self):
        return self._jconf.isFailOnUnsupportedSparkParamEnabled()

    def jks(self):
        return self._get_option(self._jconf.jks())

    def jks_pass(self):
        warnings.warn("Method 'jks_pass' is deprecated and will be removed in the next major release. Please use 'jksPass'.")
        return self.jksPass()

    def jksPass(self):
        return self._get_option(self._jconf.jksPass())

    def jks_alias(self):
        warnings.warn("Method 'jks_alias' is deprecated and will be removed in the next major release. Please use 'jksAlias'.")
        return self.jksAlias()

    def jksAlias(self):
        return self._get_option(self._jconf.jksAlias())

    def hash_login(self):
        warnings.warn("Method 'hash_login' is deprecated and will be removed in the next major release. Please use 'hashLogin'.")
        return self.hashLogin()

    def hashLogin(self):
        return self._jconf.hashLogin()

    def ldap_login(self):
        warnings.warn("Method 'ldap_login' is deprecated and will be removed in the next major release. Please use 'ldapLogin'.")
        return self.ldapLogin()

    def ldapLogin(self):
        return self._jconf.ldapLogin()

    def kerberos_login(self):
        warnings.warn("Method 'kerberos_login' is deprecated and will be removed in the next major release. Please use 'kerberosLogin'.")
        return self.kerberosLogin()

    def kerberosLogin(self):
        return self._jconf.kerberosLogin()

    def login_conf(self):
        warnings.warn("Method 'login_conf' is deprecated and will be removed in the next major release. Please use 'loginConf'.")
        return self.loginConf()

    def loginConf(self):
        return self._get_option(self._jconf.loginConf())

    def userName(self):
        return self._get_option(self._jconf.userName())

    def password(self):
        return self._get_option(self._jconf.password())

    def ssl_conf(self):
        warnings.warn("Method 'ssl_conf' is deprecated and will be removed in the next major release. Please use 'sslConf'.")
        return self.sslConf()

    def sslConf(self):
        return self._get_option(self._jconf.sslConf())

    def auto_flow_ssl(self):
        warnings.warn("Method 'auto_flow_ssl' is deprecated and will be removed in the next major release. Please use 'autoFlowSsl'.")
        return self.autoFlowSsl()

    def autoFlowSsl(self):
        return self._jconf.autoFlowSsl()

    def h2o_node_log_level(self):
        warnings.warn("Method 'h2o_node_log_level' is deprecated and will be removed in the next major release. Please use 'h2oNodeLogLevel'.")
        return self.h2oNodeLogLevel()

    def h2oNodeLogLevel(self):
        return self._jconf.h2oNodeLogLevel()

    def h2o_node_log_dir(self):
        warnings.warn("Method 'h2o_node_log_dir' is deprecated and will be removed in the next major release. Please use 'h2oNodeLogDir'.")
        return self.h2oNodeLogDir()

    def h2oNodeLogDir(self):
        return self._jconf.h2oNodeLogDir()

    def backendHeartbeatInterval(self):
        return self._jconf.backendHeartbeatInterval()

    def cloud_timeout(self):
        warnings.warn("Method 'cloud_timeout' is deprecated and will be removed in the next major release. Please use 'cloudTimeout'.")
        return self.cloudTimeout()

    def cloudTimeout(self):
        return self._jconf.cloudTimeout()

    def node_network_mask(self):
        warnings.warn("Method 'node_network_mask' is deprecated and will be removed in the next major release. Please use 'nodeNetworkMask'.")
        return self.nodeNetworkMask()

    def nodeNetworkMask(self):
        return self._get_option(self._jconf.nodeNetworkMask())

    def stacktrace_collector_interval(self):
        warnings.warn("Method 'stacktrace_collector_interval' is deprecated and will be removed in the next major release. Please use 'stacktraceCollectorInterval'.")
        return self.stacktraceCollectorInterval()

    def stacktraceCollectorInterval(self):
        return self._jconf.stacktraceCollectorInterval()

    def context_path(self):
        warnings.warn("Method 'context_path' is deprecated and will be removed in the next major release. Please use 'contextPath'.")
        return self.contextPath()

    def contextPath(self):
        return self._get_option(self._jconf.contextPath())

    def flow_scala_cell_async(self):
        warnings.warn("Method 'flow_scala_cell_async' is deprecated and will be removed in the next major release. Please use 'flowScalaCellAsync'.")
        return self.flowScalaCellAsync()

    def flowScalaCellAsync(self):
        return self._jconf.flowScalaCellAsync()

    def max_parallel_scala_cell_jobs(self):
        warnings.warn("Method 'max_parallel_scala_cell_jobs' is deprecated and will be removed in the next major release. Please use 'maxParallelScalaCellJobs'.")
        return self.maxParallelScalaCellJobs()

    def maxParallelScalaCellJobs(self):
        return self._jconf.maxParallelScalaCellJobs()

    def internal_port_offset(self):
        warnings.warn("Method 'internal_port_offset' is deprecated and will be removed in the next major release. Please use 'internalPortOffset'.")
        return self.internalPortOffset()

    def internalPortOffset(self):
        return self._jconf.internalPortOffset()

    def mojo_destroy_timeout(self):
        warnings.warn("Method 'mojo_destroy_timeout' is deprecated and will be removed in the next major release. Please use 'mojoDestroyTimeout'.")
        return self.mojoDestroyTimeout()

    def mojoDestroyTimeout(self):
        return self._jconf.mojoDestroyTimeout()

    def node_base_port(self):
        warnings.warn("Method 'node_base_port' is deprecated and will be removed in the next major release. Please use 'nodeBasePort'.")
        return self.nodeBasePort()

    def nodeBasePort(self):
        return self._jconf.nodeBasePort()

    def node_extra_properties(self):
        warnings.warn("Method 'node_extra_properties' is deprecated and will be removed in the next major release. Please use 'nodeExtraProperties'.")
        return self.nodeExtraProperties()

    def nodeExtraProperties(self):
        return self._get_option(self._jconf.nodeExtraProperties())

    def flow_extra_http_headers(self):
        warnings.warn("Method 'flow_extra_http_headers' is deprecated and will be removed in the next major release. Please use 'flowExtraHttpHeaders'.")
        return self.flowExtraHttpHeaders()

    def flowExtraHttpHeaders(self):
        return self._get_option(self._jconf.flowExtraHttpHeaders())

    def is_internal_secure_connections_enabled(self):
        warnings.warn("Method 'is_internal_secure_connections_enabled' is deprecated and will be removed in the next major release. Please use 'isInternalSecureConnectionsEnabled'.")
        return self.isInternalSecureConnectionsEnabled()

    def isInternalSecureConnectionsEnabled(self):
        return self._jconf.isInternalSecureConnectionsEnabled()

    def flow_dir(self):
        warnings.warn("Method 'flow_dir' is deprecated and will be removed in the next major release. Please use 'flowDir'.")
        return self.flowDir()

    def flowDir(self):
        return self._get_option(self._jconf.flowDir())

    def client_ip(self):
        warnings.warn("Method 'client_ip' is deprecated and will be removed in the next major release. Please use 'clientIp'.")
        return self.clientIp()

    def clientIp(self):
        return self._get_option(self._jconf.clientIp())

    def client_iced_dir(self):
        warnings.warn("Method 'client_iced_dir' is deprecated and will be removed in the next major release. Please use 'clientIcedDir'.")
        return self.clientIcedDir()

    def clientIcedDir(self):
        return self._get_option(self._jconf.clientIcedDir())

    def h2o_client_log_level(self):
        warnings.warn("Method 'h2o_client_log_level' is deprecated and will be removed in the next major release. Please use 'h2oClientLogLevel'.")
        return self.h2oClientLogLevel()

    def h2oClientLogLevel(self):
        return self._jconf.h2oClientLogLevel()

    def h2o_client_log_dir(self):
        warnings.warn("Method 'h2o_client_log_dir' is deprecated and will be removed in the next major release. Please use 'h2oClientLogDir'.")
        return self.h2oClientLogDir()

    def h2oClientLogDir(self):
        return self._get_option(self._jconf.h2oClientLogDir())

    def client_base_port(self):
        warnings.warn("Method 'client_base_port' is deprecated and will be removed in the next major release. Please use 'clientBasePort'.")
        return self.clientBasePort()

    def clientBasePort(self):
        return self._jconf.clientBasePort()

    def client_web_port(self):
        warnings.warn("Method 'client_web_port' is deprecated and will be removed in the next major release. Please use 'clientWebPort'.")
        return self.clientWebPort()

    def clientWebPort(self):
        return self._jconf.clientWebPort()

    def client_verbose_output(self):
        warnings.warn("Method 'client_verbose_output' is deprecated and will be removed in the next major release. Please use 'clientVerboseOutput'.")
        return self.clientVerboseOutput()

    def clientVerboseOutput(self):
        return self._jconf.clientVerboseOutput()

    def client_network_mask(self):
        warnings.warn("Method 'client_network_mask' is deprecated and will be removed in the next major release. Please use 'clientNetworkMask'.")
        return self.clientNetworkMask()

    def clientNetworkMask(self):
        return self._get_option(self._jconf.clientNetworkMask())

    def ignore_spark_public_dns(self):
        warnings.warn("Method 'ignore_spark_public_dns' is deprecated and will be removed in the next major release. Please use 'ignoreSparkPublicDNS'.")
        return self.ignoreSparkPublicDNS()

    def ignoreSparkPublicDNS(self):
        warnings.warn("Method 'ignoreSparkPublicDNS' is deprecated. It will be removed in the release 3.30 without replacement.")
        return self._jconf.ignoreSparkPublicDNS()

    def client_web_enabled(self):
        warnings.warn("Method 'client_web_enabled' is deprecated and will be removed in the next major release. Please use 'clientWebEnabled'.")
        return self.clientWebEnabled()

    def clientWebEnabled(self):
        warnings.warn("Method 'clientWebEnabled' is deprecated and will be removed in the next major release 3.30.")
        return True

    def client_flow_baseurl_override(self):
        warnings.warn("Method 'client_flow_baseurl_override' is deprecated and will be removed in the next major release. Please use 'clientFlowBaseurlOverride'.")
        return self.clientFlowBaseurlOverride()

    def clientFlowBaseurlOverride(self):
        return self._get_option(self._jconf.clientFlowBaseurlOverride())

    def client_extra_properties(self):
        warnings.warn("Method 'client_extra_properties' is deprecated and will be removed in the next major release. Please use 'clientExtraProperties'.")
        return self.clientExtraProperties()

    def clientExtraProperties(self):
        return self._get_option(self._jconf.clientExtraProperties())

    def runs_in_external_cluster_mode(self):
        warnings.warn("Method 'runs_in_external_cluster_mode' is deprecated and will be removed in the next major release. Please use 'runsInExternalClusterMode'.")
        return self.runsInExternalClusterMode()

    def runsInExternalClusterMode(self):
        return self._jconf.runsInExternalClusterMode()

    def runs_in_internal_cluster_mode(self):
        warnings.warn("Method 'runs_in_internal_cluster_mode' is deprecated and will be removed in the next major release. Please use 'runsInInternalClusterMode'.")
        return self.runsInInternalClusterMode()

    def runsInInternalClusterMode(self):
        return self._jconf.runsInInternalClusterMode()

    def client_check_retry_timeout(self):
        warnings.warn("Method 'client_check_retry_timeout' is deprecated and will be removed in the next major release. Please use 'clientCheckRetryTimeout'.")
        return self.clientCheckRetryTimeout()

    def clientCheckRetryTimeout(self):
        return self._jconf.clientCheckRetryTimeout()

    def verifySslCertificates(self):
        return self._jconf.verifySslCertificates()

    def isHiveSupportEnabled(self):
        return self._jconf.isHiveSupportEnabled()

    def hiveHost(self):
        return self._get_option(self._jconf.hiveHost())

    def hivePrincipal(self):
        return self._get_option(self._jconf.hivePrincipal())

    def hiveJdbcUrlPattern(self):
        return self._get_option(self._jconf.hiveJdbcUrlPattern())

    def hiveToken(self):
        return self._get_option(self._jconf.hiveToken())

    #
    # Setters
    #

    def set_internal_cluster_mode(self):
        warnings.warn("Method 'set_internal_cluster_mode' is deprecated and will be removed in the next major release. Please use 'setInternalClusterMode'.")
        return self.setInternalClusterMode()

    def setInternalClusterMode(self):
        self._jconf.setInternalClusterMode()
        return self

    def set_external_cluster_mode(self):
        warnings.warn("Method 'set_external_cluster_mode' is deprecated and will be removed in the next major release. Please use 'setExternalClusterMode'.")
        return self.setExternalClusterMode()

    def setExternalClusterMode(self):
        self._jconf.setExternalClusterMode()
        return self

    def set_cloud_name(self, cloud_name):
        warnings.warn("Method 'set_cloud_name' is deprecated and will be removed in the next major release. Please use 'setCloudName'.")
        return self.setCloudName(cloud_name)

    def setCloudName(self, cloudName):
        self._jconf.setCloudName(cloudName)
        return self

    def set_nthreads(self, nthreads):
        warnings.warn("Method 'set_nthreads' is deprecated and will be removed in the next major release. Please use 'setNthreads'.")
        return self.setNthreads(nthreads)

    def setNthreads(self, nthreads):
        self._jconf.setNthreads(nthreads)
        return self

    def set_repl_enabled(self):
        warnings.warn("Method 'set_repl_enabled' is deprecated and will be removed in the next major release. Please use 'setReplEnabled'.")
        return self.setReplEnabled()

    def setReplEnabled(self):
        self._jconf.setReplEnabled()
        return self

    def set_repl_disabled(self):
        warnings.warn("Method 'set_repl_disabled' is deprecated and will be removed in the next major release. Please use 'setReplDisabled'.")
        return self.setReplDisabled()

    def setReplDisabled(self):
        self._jconf.setReplDisabled()
        return self

    def set_default_num_repl_sessions(self, num_sessions):
        warnings.warn("Method 'set_default_num_repl_sessions' is deprecated and will be removed in the next major release. Please use 'setDefaultNumReplSessions'.")
        return self.setDefaultNumReplSessions(num_sessions)

    def setDefaultNumReplSessions(self, numSessions):
        self._jconf.setDefaultNumReplSessions(numSessions)
        return self

    def set_cluster_topology_listener_enabled(self):
        warnings.warn("Method 'set_cluster_topology_listener_enabled' is deprecated and will be removed in the next major release. Please use 'setClusterTopologyListenerEnabled'.")
        return self.setClusterTopologyListenerEnabled()

    def setClusterTopologyListenerEnabled(self):
        self._jconf.setClusterTopologyListenerEnabled()
        return self

    def set_cluster_topology_listener_disabled(self):
        warnings.warn("Method 'set_cluster_topology_listener_disabled' is deprecated and will be removed in the next major release. Please use 'setClusterTopologyListenerDisabled'.")
        return self.setClusterTopologyListenerDisabled()

    def setClusterTopologyListenerDisabled(self):
        self._jconf.setClusterTopologyListenerDisabled()
        return self

    def setSparkVersionCheckEnabled(self):
        self._jconf.setSparkVersionCheckEnabled()
        return self

    def set_spark_version_check_disabled(self):
        warnings.warn("Method 'set_spark_version_check_disabled' is deprecated and will be removed in the next major release. Please use 'setSparkVersionCheckDisabled'.")
        return self.setSparkVersionCheckDisabled()

    def setSparkVersionCheckDisabled(self):
        self._jconf.setSparkVersionCheckDisabled()
        return self

    def set_fail_on_unsupported_spark_param_enabled(self):
        warnings.warn("Method 'set_fail_on_unsupported_spark_param_enabled' is deprecated and will be removed in the next major release. Please use 'setFailOnUnsupportedSparkParamEnabled'.")
        return self.setFailOnUnsupportedSparkParamEnabled()

    def setFailOnUnsupportedSparkParamEnabled(self):
        self._jconf.setFailOnUnsupportedSparkParamEnabled()
        return self

    def set_fail_on_unsupported_spark_param_disabled(self):
        warnings.warn("Method 'set_fail_on_unsupported_spark_param_disabled' is deprecated and will be removed in the next major release. Please use 'setFailOnUnsupportedSparkParamDisabled'.")
        return self.setFailOnUnsupportedSparkParamDisabled()

    def setFailOnUnsupportedSparkParamDisabled(self):
        self._jconf.setFailOnUnsupportedSparkParamDisabled()
        return self

    def set_jks(self, path):
        warnings.warn("Method 'set_jks' is deprecated and will be removed in the next major release. Please use 'setJks'.")
        return self.setJks(path)

    def setJks(self, path):
        self._jconf.setJks(path)
        return self

    def set_jks_pass(self, password):
        warnings.warn("Method 'set_jks_pass' is deprecated and will be removed in the next major release. Please use 'setJksPass'.")
        return self.setJksPass(password)

    def setJksPass(self, password):
        self._jconf.setJksPass(password)
        return self

    def set_jks_alias(self, alias):
        warnings.warn("Method 'set_jks_alias' is deprecated and will be removed in the next major release. Please use 'setJksAlias'.")
        return self.setJksAlias(alias)

    def setJksAlias(self, alias):
        self._jconf.setJksAlias(alias)
        return self

    def set_hash_login_enabled(self):
        warnings.warn("Method 'set_hash_login_enabled' is deprecated and will be removed in the next major release. Please use 'setHashLoginEnabled'.")
        return self.setHashLoginEnabled()

    def setHashLoginEnabled(self):
        self._jconf.setHashLoginEnabled()
        return self

    def set_hash_login_disabled(self):
        warnings.warn("Method 'set_hash_login_disabled' is deprecated and will be removed in the next major release. Please use 'setHashLoginDisabled'.")
        return self.setHashLoginDisabled()

    def setHashLoginDisabled(self):
        self._jconf.setHashLoginDisabled()
        return self

    def set_ldap_login_enabled(self):
        warnings.warn("Method 'set_ldap_login_enabled' is deprecated and will be removed in the next major release. Please use 'setLdapLoginEnabled'.")
        return self.setLdapLoginEnabled()

    def setLdapLoginEnabled(self):
        self._jconf.setLdapLoginEnabled()
        return self

    def set_ldap_login_disabled(self):
        warnings.warn("Method 'set_ldap_login_disabled' is deprecated and will be removed in the next major release. Please use 'setLdapLoginDisabled'.")
        return self.setLdapLoginDisabled()

    def setLdapLoginDisabled(self):
        self._jconf.setLdapLoginDisabled()
        return self

    def set_kerberos_login_enabled(self):
        warnings.warn("Method 'set_kerberos_login_enabled' is deprecated and will be removed in the next major release. Please use 'setKerberosLoginEnabled'.")
        return self.setKerberosLoginEnabled()

    def setKerberosLoginEnabled(self):
        self._jconf.setKerberosLoginEnabled()
        return self

    def set_kerberos_login_disabled(self):
        warnings.warn("Method 'set_kerberos_login_disabled' is deprecated and will be removed in the next major release. Please use 'setKerberosLoginDisabled'.")
        return self.setKerberosLoginDisabled()

    def setKerberosLoginDisabled(self):
        self._jconf.setKerberosLoginDisabled()
        return self

    def set_login_conf(self, file):
        warnings.warn("Method 'set_login_conf' is deprecated and will be removed in the next major release. Please use 'setLoginConf'.")
        return self.setLoginConf(file)

    def setLoginConf(self, filePath):
        self._jconf.setLoginConf(filePath)
        return self

    def setUserName(self, username):
        self._jconf.setUserName(username)
        return self

    def setPassword(self, password):
        self._jconf.setPassword(password)
        return self

    def set_ssl_conf(self, path):
        warnings.warn("Method 'set_ssl_conf' is deprecated and will be removed in the next major release. Please use 'setSslConf'.")
        return self.setSslConf(path)

    def setSslConf(self, path):
        self._jconf.setSslConf(path)
        return self

    def set_auto_flow_ssl_enabled(self):
        warnings.warn("Method 'set_auto_flow_ssl_enabled' is deprecated and will be removed in the next major release. Please use 'setAutoFlowSslEnabled'.")
        return self.setAutoFlowSslEnabled()

    def setAutoFlowSslEnabled(self):
        self._jconf.setAutoFlowSslEnabled()
        return self

    def set_auto_flow_ssl_disabled(self):
        warnings.warn("Method 'set_auto_flow_ssl_disabled' is deprecated and will be removed in the next major release. Please use 'setAutoFlowSslDisabled'.")
        return self.setAutoFlowSslDisabled()

    def setAutoFlowSslDisabled(self):
        self._jconf.setAutoFlowSslDisabled()
        return self

    def set_h2o_node_log_level(self, level):
        warnings.warn("Method 'set_h2o_node_log_level' is deprecated and will be removed in the next major release. Please use 'setH2ONodeLogLevel'.")
        return self.setH2ONodeLogLevel(level)

    def setH2ONodeLogLevel(self, level):
        self._jconf.setH2ONodeLogLevel(level)
        return self

    def set_h2o_node_log_dir(self, dir):
        warnings.warn("Method 'set_h2o_node_log_dir' is deprecated and will be removed in the next major release. Please use 'setH2ONodeLogDir'.")
        return self.setH2ONodeLogDir(dir)

    def setH2ONodeLogDir(self, dir):
        self._jconf.setH2ONodeLogDir(dir)
        return self

    def setBackendHeartbeatInterval(self, interval):
        self._jconf.setBackendHeartbeatInterval(interval)
        return self

    def set_cloud_timeout(self, timeout):
        warnings.warn("Method 'set_cloud_timeout' is deprecated and will be removed in the next major release. Please use 'setCloudTimeout'.")
        return self.setCloudTimeout(timeout)

    def setCloudTimeout(self, timeout):
        self._jconf.setCloudTimeout(timeout)
        return self

    def set_node_network_mask(self, mask):
        warnings.warn("Method 'set_node_network_mask' is deprecated and will be removed in the next major release. Please use 'setNodeNetworkMask'.")
        return self.setNodeNetworkMask(mask)

    def setNodeNetworkMask(self, mask):
        self._jconf.setNodeNetworkMask(mask)
        return self

    def set_stacktrace_collector_interval(self, interval):
        warnings.warn("Method 'set_stacktrace_collector_interval' is deprecated and will be removed in the next major release. Please use 'setStacktraceCollectorInterval'.")
        return self.setStacktraceCollectorInterval(interval)

    def setStacktraceCollectorInterval(self, interval):
        self._jconf.setStacktraceCollectorInterval(interval)
        return self

    def set_context_path(self, context_path):
        warnings.warn("Method 'set_context_path' is deprecated and will be removed in the next major release. Please use 'setContextPath'.")
        return self.setContextPath(context_path)

    def setContextPath(self, contextPath):
        self._jconf.setContextPath(contextPath)
        return self

    def set_flow_scala_cell_async_enabled(self):
        warnings.warn("Method 'set_flow_scala_cell_async_enabled' is deprecated and will be removed in the next major release. Please use 'setFlowScalaCellAsyncEnabled'.")
        return self.setFlowScalaCellAsyncEnabled()

    def setFlowScalaCellAsyncEnabled(self):
        self._jconf.setFlowScalaCellAsyncEnabled()
        return self

    def set_flow_scala_cell_async_disabled(self):
        warnings.warn("Method 'set_flow_scala_cell_async_disabled' is deprecated and will be removed in the next major release. Please use 'setFlowScalaCellAsyncDisabled'.")
        return self.setFlowScalaCellAsyncDisabled()

    def setFlowScalaCellAsyncDisabled(self):
        self._jconf.setFlowScalaCellAsyncDisabled()
        return self

    def set_max_parallel_scala_cell_jobs(self, limit):
        warnings.warn("Method 'set_max_parallel_scala_cell_jobs' is deprecated and will be removed in the next major release. Please use 'setMaxParallelScalaCellJobs'.")
        return self.setMaxParallelScalaCellJobs(limit)

    def setMaxParallelScalaCellJobs(self, limit):
        self._jconf.setMaxParallelScalaCellJobs(limit)
        return self

    def set_internal_port_offset(self, offset):
        warnings.warn("Method 'set_internal_port_offset' is deprecated and will be removed in the next major release. Please use 'setInternalPortOffset'.")
        return self.setInternalPortOffset(offset)

    def setInternalPortOffset(self, offset):
        self._jconf.setInternalPortOffset(offset)
        return self

    def set_node_base_port(self, port):
        warnings.warn("Method 'set_node_base_port' is deprecated and will be removed in the next major release. Please use 'setNodeBasePort'.")
        return self.setNodeBasePort(port)

    def setNodeBasePort(self, port):
        self._jconf.setNodeBasePort(port)
        return self

    def set_mojo_destroy_timeout(self, timeoutInMilliseconds):
        warnings.warn("Method 'set_mojo_destroy_timeout' is deprecated and will be removed in the next major release. Please use 'setMojoDestroyTimeout'.")
        return self.setMojoDestroyTimeout(timeoutInMilliseconds)

    def setMojoDestroyTimeout(self, timeoutInMilliseconds):
        self._jconf.setMojoDestroyTimeout(timeoutInMilliseconds)
        return self

    def set_node_extra_properties(self, extraProperties):
        warnings.warn("Method 'set_node_extra_properties' is deprecated and will be removed in the next major release. Please use 'setNodeExtraProperties'.")
        return self.setNodeExtraProperties(extraProperties)

    def setNodeExtraProperties(self, extraProperties):
        self._jconf.setNodeExtraProperties(extraProperties)
        return self

    def set_flow_extra_http_headers(self, headers):
        warnings.warn("Method 'set_flow_extra_http_headers' is deprecated and will be removed in the next major release. Please use 'setFlowExtraHttpHeaders'.")
        return self.setFlowExtraHttpHeaders(headers)

    def setFlowExtraHttpHeaders(self, headers):
        self._jconf.setFlowExtraHttpHeaders(headers)
        return self

    def set_internal_secure_connections_enabled(self):
        warnings.warn("Method 'set_internal_secure_connections_enabled' is deprecated and will be removed in the next major release. Please use 'setInternalSecureConnectionsEnabled'.")
        return self.setInternalSecureConnectionsEnabled()

    def setInternalSecureConnectionsEnabled(self):
        self._jconf.setInternalSecureConnectionsEnabled()
        return self

    def set_internal_secure_connections_disabled(self):
        warnings.warn("Method 'set_internal_secure_connections_disabled' is deprecated and will be removed in the next major release. Please use 'setInternalSecureConnectionsDisabled'.")
        return self.setInternalSecureConnectionsDisabled()

    def setInternalSecureConnectionsDisabled(self):
        self._jconf.setInternalSecureConnectionsDisabled()
        return self

    def set_flow_dir(self, dir):
        warnings.warn("Method 'set_flow_dir' is deprecated and will be removed in the next major release. Please use 'setFlowDir'.")
        return self.setFlowDir(dir)

    def setFlowDir(self, dir):
        self._jconf.setFlowDir(dir)
        return self

    def set_client_ip(self, ip):
        warnings.warn("Method 'set_client_ip' is deprecated and will be removed in the next major release. Please use 'setClientIp'.")
        return self.setClientIp(ip)

    def setClientIp(self, ip):
        self._jconf.setClientIp(ip)
        return self

    def set_client_iced_dir(self, iced_dir):
        warnings.warn("Method 'set_client_iced_dir' is deprecated and will be removed in the next major release. Please use 'setClientIcedDir'.")
        return self.setClientIcedDir(iced_dir)

    def setClientIcedDir(self, icedDir):
        self._jconf.setClientIcedDir(icedDir)
        return self

    def set_h2o_client_log_level(self, level):
        warnings.warn("Method 'set_h2o_client_log_level' is deprecated and will be removed in the next major release. Please use 'setH2OClientLogLevel'.")
        return self.setH2OClientLogLevel(level)

    def setH2OClientLogLevel(self, level):
        self._jconf.setH2OClientLogLevel(level)
        return self

    def set_h2o_client_log_dir(self, dir):
        warnings.warn("Method 'set_h2o_client_log_dir' is deprecated and will be removed in the next major release. Please use 'setH2OClientLogDir'.")
        return self.setH2OClientLogDir(dir)

    def setH2OClientLogDir(self, dir):
        self._jconf.setH2OClientLogDir(dir)
        return self

    def set_client_port_base(self, baseport):
        warnings.warn("Method 'set_client_port_base' is deprecated and will be removed in the next major release. Please use 'setClientPortBase'.")
        return self.setClientPortBase(baseport)

    def setClientPortBase(self, basePort):
        warnings.warn("The method 'setClientPortBase' has been deprecated and will be removed in 3.30."
                      " Use the 'setClientBasePort' method instead.")
        self.setClientBasePort(basePort)
        return self

    def setClientBasePort(self, basePort):
        self._jconf.setClientBasePort(basePort)
        return self

    def set_client_web_port(self, port):
        warnings.warn("Method 'set_client_web_port' is deprecated and will be removed in the next major release. Please use 'setClientWebPort'.")
        return self.setClientWebPort(port)

    def setClientWebPort(self, port):
        self._jconf.setClientWebPort(port)
        return self

    def set_client_verbose_enabled(self):
        warnings.warn("Method 'set_client_verbose_enabled' is deprecated and will be removed in the next major release. Please use 'setClientVerboseEnabled'.")
        return self.setClientVerboseEnabled()

    def setClientVerboseEnabled(self):
        self._jconf.setClientVerboseEnabled()
        return self

    def set_client_verbose_disabled(self):
        warnings.warn("Method 'set_client_verbose_disabled' is deprecated and will be removed in the next major release. Please use 'setClientVerboseDisabled'.")
        return self.setClientVerboseDisabled()

    def setClientVerboseDisabled(self):
        self._jconf.setClientVerboseDisabled()
        return self

    def set_client_network_mask(self, mask):
        warnings.warn("Method 'set_client_network_mask' is deprecated and will be removed in the next major release. Please use 'setClientNetworkMask'.")
        return self.setClientNetworkMask(mask)

    def setClientNetworkMask(self, mask):
        self._jconf.setClientNetworkMask(mask)
        return self

    def set_ignore_spark_public_dns_enabled(self):
        warnings.warn("Method 'set_ignore_spark_public_dns_enabled' is deprecated and will be removed in the next major release. Please use 'setIgnoreSparkPublicDNSEnabled'.")
        return self.setIgnoreSparkPublicDNSEnabled()

    def setIgnoreSparkPublicDNSEnabled(self):
        warnings.warn("Method 'etIgnoreSparkPublicDNSEnabled' is deprecated. It will be removed in the release 3.30 without replacement.")
        self._jconf.setIgnoreSparkPublicDNSEnabled()
        return self

    def set_ignore_spark_public_dns_disabled(self):
        warnings.warn("Method 'set_ignore_spark_public_dns_disabled' is deprecated and will be removed in the next major release. Please use 'setIgnoreSparkPublicDNSDisabled'.")
        return self.setIgnoreSparkPublicDNSDisabled()

    def setIgnoreSparkPublicDNSDisabled(self):
        warnings.warn("Method 'setIgnoreSparkPublicDNSDisabled' is deprecated. It will be removed in the release 3.30 without replacement.")
        self._jconf.setIgnoreSparkPublicDNSDisabled()
        return self

    def set_client_web_enabled(self):
        warnings.warn("Method 'set_client_web_enabled' is deprecated and will be removed in the next major release. Please use 'setClientWebEnabled'.")
        return self.setClientWebEnabled()

    def setClientWebEnabled(self):
        warnings.warn("Method 'setClientWebEnabled' is deprecated and will be removed in the next major release 3.30.")
        return self

    def set_client_web_disabled(self):
        warnings.warn("Method 'set_client_web_disabled' is deprecated and will be removed in the next major release. Please use 'setClientWebDisabled'.")
        return self.setClientWebDisabled()

    def setClientWebDisabled(self):
        warnings.warn("Method 'setClientWebDisabled' is deprecated and will be removed in the next major release 3.30.")
        return self

    def set_client_flow_baseurl_override(self, value):
        warnings.warn("Method 'set_client_flow_baseurl_override' is deprecated and will be removed in the next major release. Please use 'setClientFlowBaseurlOverride'.")
        return self.setClientFlowBaseurlOverride(value)

    def setClientFlowBaseurlOverride(self, baseUrl):
        self._jconf.setClientFlowBaseurlOverride(baseUrl)
        return self

    def set_client_check_retry_timeout(self, timeout):
        warnings.warn("Method 'set_client_check_retry_timeout' is deprecated and will be removed in the next major release. Please use 'setClientCheckRetryTimeout'.")
        return self.setClientCheckRetryTimeout(timeout)

    def setClientCheckRetryTimeout(self, timeout):
        self._jconf.setClientCheckRetryTimeout(timeout)
        return self

    def set_client_extra_properties(self, extraProperties):
        warnings.warn("Method 'set_client_extra_properties' is deprecated and will be removed in the next major release. Please use 'setClientExtraProperties'.")
        return self.setClientExtraProperties(extraProperties)

    def setClientExtraProperties(self, extraProperties):
        self._jconf.setClientExtraProperties(extraProperties)
        return self

    def setVerifySslCertificates(self, verify):
        self._jconf.setVerifySslCertificates(verify)
        return self

    def setHiveSupportEnabled(self):
        self._jconf.setHiveSupportEnabled()
        return self

    def setHiveSupportDisabled(self):
        self._jconf.setHiveSupportDisabled()
        return self

    def setHiveHost(self, host):
        self._jconf.setHiveHost(host)
        return self

    def setHivePrincipal(self, principal):
        self._jconf.setHivePrincipal(principal)
        return self

    def setHiveJdbcUrlPattern(self, pattern):
        self._jconf.setHiveJdbcUrlPattern(pattern)
        return self

    def setHiveToken(self, token):
        self._jconf.setHiveToken(token)
        return self
