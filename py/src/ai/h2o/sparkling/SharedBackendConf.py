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

from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils


class SharedBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def backend_cluster_mode(self):
        return self._jconf.backendClusterMode()

    def runs_in_external_cluster_mode(self):
        return self._jconf.runsInExternalClusterMode()

    def runs_in_internal_cluster_mode(self):
        return self._jconf.runsInInternalClusterMode()

    def cloud_name(self):
        return self._get_option(self._jconf.cloudName())

    def nthreads(self):
        return self._jconf.nthreads()

    def is_h2o_repl_enabled(self):
        return self._jconf.isH2OReplEnabled()

    def scala_int_default_num(self):
        return self._jconf.scalaIntDefaultNum()

    def is_cluster_topology_listener_enabled(self):
        return self._jconf.isClusterTopologyListenerEnabled()

    def is_spark_version_check_enabled(self):
        return self._jconf.isSparkVersionCheckEnabled()

    def is_fail_on_unsupported_spark_param_enabled(self):
        return self._jconf.isFailOnUnsupportedSparkParamEnabled()

    def jks(self):
        return self._get_option(self._jconf.jks())

    def jks_pass(self):
        return self._get_option(self._jconf.jksPass())

    def jks_alias(self):
        return self._get_option(self._jconf.jksAlias())

    def hash_login(self):
        return self._jconf.hashLogin()

    def ldap_login(self):
        return self._jconf.ldapLogin()

    def kerberos_login(self):
        return self._jconf.kerberosLogin()

    def login_conf(self):
        return self._get_option(self._jconf.loginConf())

    def userName(self):
        return self._get_option(self._jconf.userName())

    def password(self):
        return self._get_option(self._jconf.password())

    def ssl_conf(self):
        return self._get_option(self._jconf.sslConf())

    def auto_flow_ssl(self):
        return self._jconf.autoFlowSsl()

    def h2o_node_log_level(self):
        return self._jconf.h2oNodeLogLevel()

    def h2o_node_log_dir(self):
        return self._jconf.h2oNodeLogDir()

    def backendHeartbeatInterval(self):
        return self._jconf.backendHeartbeatInterval()

    def cloud_timeout(self):
        return self._jconf.cloudTimeout()

    def h2o_node_web_enabled(self):
        return self._jconf.h2oNodeWebEnabled()

    def node_network_mask(self):
        return self._get_option(self._jconf.nodeNetworkMask())

    def stacktrace_collector_interval(self):
        return self._jconf.stacktraceCollectorInterval()

    def context_path(self):
        return self._get_option(self._jconf.contextPath())

    def flow_scala_cell_async(self):
        return self._jconf.flowScalaCellAsync()

    def max_parallel_scala_cell_jobs(self):
        return self._jconf.maxParallelScalaCellJobs()

    def internal_port_offset(self):
        return self._jconf.internalPortOffset()

    def mojo_destroy_timeout(self):
        return self._jconf.mojoDestroyTimeout()

    def node_base_port(self):
        return self._jconf.nodeBasePort()

    def node_extra_properties(self):
        return self._get_option(self._jconf.nodeExtraProperties())

    def flow_extra_http_headers(self):
        return self._get_option(self._jconf.flowExtraHttpHeaders())

    def is_internal_secure_connections_enabled(self):
        return self._jconf.isInternalSecureConnectionsEnabled()

    def flow_dir(self):
        return self._get_option(self._jconf.flowDir())

    def client_ip(self):
        return self._get_option(self._jconf.clientIp())

    def client_iced_dir(self):
        return self._get_option(self._jconf.clientIcedDir())

    def h2o_client_log_level(self):
        return self._jconf.h2oClientLogLevel()

    def h2o_client_log_dir(self):
        return self._get_option(self._jconf.h2oClientLogDir())

    def client_base_port(self):
        return self._jconf.clientBasePort()

    def client_web_port(self):
        return self._jconf.clientWebPort()

    def client_verbose_output(self):
        return self._jconf.clientVerboseOutput()

    def client_network_mask(self):
        return self._get_option(self._jconf.clientNetworkMask())

    def ignore_spark_public_dns(self):
        return self._jconf.ignoreSparkPublicDNS()

    def client_web_enabled(self):
        return self._jconf.clientWebEnabled()

    def client_flow_baseurl_override(self):
        return self._get_option(self._jconf.clientFlowBaseurlOverride())

    def client_extra_properties(self):
        return self._get_option(self._jconf.clientExtraProperties())

    #
    # Setters
    #

    def set_internal_cluster_mode(self):
        self._jconf.setInternalClusterMode()
        return self

    def set_external_cluster_mode(self):
        self._jconf.setExternalClusterMode()
        return self

    def set_cloud_name(self, cloud_name):
        self._jconf.setCloudName(cloud_name)
        return self

    def set_nthreads(self, nthreads):
        self._jconf.setNthreads(nthreads)
        return self

    def set_repl_enabled(self):
        self._jconf.setReplEnabled()
        return self

    def set_repl_disabled(self):
        self._jconf.setReplDisabled()
        return self

    def set_default_num_repl_sessions(self, num_sessions):
        self._jconf.setDefaultNumReplSessions(num_sessions)
        return self

    def set_cluster_topology_listener_enabled(self):
        self._jconf.setClusterTopologyListenerEnabled()
        return self

    def set_cluster_topology_listener_disabled(self):
        self._jconf.setClusterTopologyListenerDisabled()
        return self

    def set_spark_version_check_enabled(self):
        self._jconf.setSparkVersionCheckEnabled()
        return self

    def set_spark_version_check_disabled(self):
        self._jconf.setSparkVersionCheckDisabled()
        return self

    def set_fail_on_unsupported_spark_param_enabled(self):
        self._jconf.setFailOnUnsupportedSparkParamEnabled()
        return self

    def set_fail_on_unsupported_spark_param_disabled(self):
        self._jconf.setFailOnUnsupportedSparkParamDisabled()
        return self

    def set_jks(self, path):
        self._jconf.setJks(path)
        return self

    def set_jks_pass(self, password):
        self._jconf.setJksPass(password)
        return self

    def set_jks_alias(self, alias):
        self._jconf.setJksAlias(alias)
        return self

    def set_hash_login_enabled(self):
        self._jconf.setHashLoginEnabled()
        return self

    def set_hash_login_disabled(self):
        self._jconf.setHashLoginDisabled()
        return self

    def set_ldap_login_enabled(self):
        self._jconf.setLdapLoginEnabled()
        return self

    def set_ldap_login_disabled(self):
        self._jconf.setLdapLoginDisabled()
        return self

    def set_kerberos_login_enabled(self):
        self._jconf.setKerberosLoginEnabled()
        return self

    def set_kerberos_login_disabled(self):
        self._jconf.setKerberosLoginDisabled()
        return self

    def set_login_conf(self, file):
        self._jconf.setLoginConf(file)
        return self

    def setUserName(self, username):
        self._jconf.setUserName(username)
        return self

    def setPassword(self, password):
        self._jconf.setPassword(password)
        return self

    def set_ssl_conf(self, path):
        self._jconf.setSslConf(path)
        return self

    def set_auto_flow_ssl_enabled(self):
        self._jconf.setAutoFlowSslEnabled()
        return self

    def set_auto_flow_ssl_disabled(self):
        self._jconf.setAutoFlowSslDisabled()
        return self

    def set_h2o_node_log_level(self, level):
        self._jconf.setH2ONodeLogLevel(level)
        return self

    def set_h2o_node_log_dir(self, dir):
        self._jconf.setH2ONodeLogDir(dir)
        return self

    def setBackendHeartbeatInterval(self, interval):
        self._jconf.setBackendHeartbeatInterval(interval)
        return self

    def set_cloud_timeout(self, timeout):
        self._jconf.setCloudTimeout(timeout)
        return self

    def set_h2o_node_web_enabled(self):
        self._jconf.setH2ONodeWebEnabled()
        return self

    def set_h2o_node_web_disabled(self):
        self._jconf.setH2ONodeWebDisabled()
        return self

    def set_node_network_mask(self, mask):
        self._jconf.setNodeNetworkMask(mask)
        return self

    def set_stacktrace_collector_interval(self, interval):
        self._jconf.setStacktraceCollectorInterval(interval)
        return self

    def set_context_path(self, context_path):
        self._jconf.setContextPath(context_path)
        return self

    def set_flow_scala_cell_async_enabled(self):
        self._jconf.setFlowScalaCellAsyncEnabled()
        return self

    def set_flow_scala_cell_async_disabled(self):
        self._jconf.setFlowScalaCellAsyncDisabled()
        return self

    def set_max_parallel_scala_cell_jobs(self, limit):
        self._jconf.setMaxParallelScalaCellJobs(limit)
        return self

    def set_internal_port_offset(self, offset):
        self._jconf.setInternalPortOffset(offset)
        return self

    def set_mojo_destroy_timeout(self, timeoutInMilliseconds):
        self._jconf.setMojoDestroyTimeout(timeoutInMilliseconds)
        return self

    def set_node_base_port(self, port):
        self._jconf.setNodeBasePort(port)
        return self

    def set_node_extra_properties(self, extraProperties):
        self._jconf.setNodeExtraProperties(extraProperties)
        return self

    def set_flow_extra_http_headers(self, headers):
        self._jconf.setFlowExtraHttpHeaders(headers)
        return self

    def set_internal_secure_connections_enabled(self):
        self._jconf.setInternalSecureConnectionsEnabled()
        return self

    def set_internal_secure_connections_disabled(self):
        self._jconf.setInternalSecureConnectionsDisabled()
        return self

    def set_flow_dir(self, dir):
        self._jconf.setFlowDir(dir)
        return self

    def set_client_ip(self, ip):
        self._jconf.setClientIp(ip)
        return self

    def set_client_iced_dir(self, iced_dir):
        self._jconf.setClientIcedDir(iced_dir)
        return self

    def set_h2o_client_log_level(self, level):
        self._jconf.setH2OClientLogLevel(level)
        return self

    def set_h2o_client_log_dir(self, dir):
        self._jconf.setH2OClientLogDir(dir)
        return self

    def set_client_port_base(self, baseport):
        self._jconf.setClientPortBase(baseport)
        return self

    def set_client_web_port(self, port):
        self._jconf.setClientWebPort(port)
        return self

    def set_client_verbose_enabled(self):
        self._jconf.setClientVerboseEnabled()
        return self

    def set_client_verbose_disabled(self):
        self._jconf.setClientVerboseDisabled()
        return self

    def set_client_network_mask(self, mask):
        self._jconf.setClientNetworkMask(mask)
        return self

    def set_ignore_spark_public_dns_enabled(self):
        self._jconf.setIgnoreSparkPublicDNSEnabled()
        return self

    def set_ignore_spark_public_dns_disabled(self):
        self._jconf.setIgnoreSparkPublicDNSDisabled()
        return self

    def set_client_web_enabled(self):
        self._jconf.setClientWebEnabled()
        return self

    def set_client_web_disabled(self):
        self._jconf.setClientWebDisabled()
        return self

    def set_client_flow_baseurl_override(self, value):
        self._jconf.setClientFlowBaseurlOverride(value)
        return self

    def set_client_extra_properties(self, extraProperties):
        self._jconf.setClientExtraProperties(extraProperties)
        return self
