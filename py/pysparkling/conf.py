from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pysparkling.initializer import Initializer
import warnings


class H2OConf(object):
    def __init__(self, spark):
        try:
            spark_session = spark
            if isinstance(spark, SparkContext):
                warnings.warn("Method H2OContext.getOrCreate with argument of type SparkContext is deprecated and " +
                              "parameter of type SparkSession is preferred.")
                spark_session = SparkSession.builder.getOrCreate()

            Initializer.load_sparkling_jar(spark_session._sc)
            self._do_init(spark_session)
        except:
            raise


    def _do_init(self, spark_session):
        self._spark_session = spark_session
        self._sc = self._spark_session._sc
        jvm = self._sc._jvm
        jsc = self._sc._jsc
        # Create instance of H2OConf class
        self._jconf = jvm.org.apache.spark.h2o.H2OConf(jsc)

    def _get_option(self, option):
        if option.isDefined():
            return option.get()
        else:
            return None


    # setters independent on selected backend

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

    def set_user_name(self, username):
        self._jconf.setUserName(username)
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

    def set_ui_update_interval(self, interval):
        self._jconf.setUiUpdateInterval(interval)
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

    # setters for internal backend

    def set_flatfile_enabled(self):
        self._jconf.setFlatFileEnabled()
        return self

    def set_flatfile_disabled(self):
        self._jconf.setFlatFileDisabled()
        return self

    def set_ip_based_faltfile_enabled(self):
        self._jconf.setIpBasedFlatFileEnabled()
        return self

    def set_ip_based_faltfile_disabled(self):
        self._jconf.setIpBasedFlatFileDisabled()
        return self

    def set_num_h2o_workers(self, num_workers):
        self._jconf.setNumH2OWorkers(num_workers)
        return self

    def set_drdd_mul_factor(self, factor):
        self._jconf.setDrddMulFactor(factor)
        return self

    def set_num_rdd_retries(self, retries):
        self._jconf.setNumRddRetries(retries)
        return self

    def set_default_cloud_size(self, size):
        self._jconf.setDefaultCloudSize(size)
        return self

    def set_subseq_tries(self, subseq_tries_num):
        self._jconf.setSubseqTries(subseq_tries_num)
        return self

    def set_node_base_port(self, port):
        self._jconf.setNodeBasePort(port)
        return self

    def set_node_iced_dir(self, dir):
        self._jconf.setNodeIcedDir(dir)
        return self

    def set_internal_secure_connections_enabled(self):
        self._jconf.setInternalSecureConnectionsEnabled()
        return self

    def set_internal_secure_connections_disabled(self):
        self._jconf.setInternalSecureConnectionsDisabled()
        return self


    # setters for external backend

    def set_h2o_cluster(self, ip, port):
        self._jconf.setH2OCluster(ip, port)
        return self

    def set_num_of_external_h2o_nodes(self, num_of_external_h2o_nodes):
        self._jconf.setNumOfExternalH2ONodes(num_of_external_h2o_nodes)
        return self

    def set_client_check_retry_timeout(self, timeout):
        """Set retry interval how often nodes in the external cluster mode check for the presence of the h2o client.

        Arguments:
        timeout -- timeout in milliseconds

        """
        self._jconf.setClientCheckRetryTimeout(timeout)
        return self

    def set_client_connection_timeout(self, timeout):
        """Set timeout for watchdog client connection in external cluster mode. If the client is not connected to the
         cluster within the specified time, the cluster kill itself.

        Arguments:
        timeout -- timeout in milliseconds

        """
        self._jconf.setClientConnectionTimeout(timeout)
        return self

    def set_external_read_confirmation_timeout(self, timeout):
        self._jconf.setExternalReadConfirmationTimeout(timeout)
        return self

    def set_external_write_confirmation_timeout(self, timeout):
        self._jconf.setExternalWriteConfirmationTimeout(timeout)
        return self

    def set_cluster_start_timeout(self, timeout):
        """Set timeout for start of external cluster. If the cluster is not able to cloud within the timeout the
        the exception is thrown.

        Arguments:
        timeout -- timeout in seconds

        """
        self._jconf.setClusterStartTimeout(timeout)
        return self


    def set_cluster_config_file(self, path):
        self._jconf.setClusterConfigFile(path)
        return self

    def set_mapper_xmx(self, mem):
        self._jconf.setMapperXmx(mem)
        return self

    def set_hdfs_output_dir(self, hdfs_output_dir):
        self._jconf.setHDFSOutputDir(hdfs_output_dir)
        return self

    def use_auto_cluster_start(self):
        self._jconf.useAutoClusterStart()
        return self

    def use_manual_cluster_start(self):
        self._jconf.useManualClusterStart()
        return self

    def set_h2o_driver_path(self, driver_path):
        self._jconf.setH2ODriverPath(driver_path)
        return self

    def set_yarn_queue(self, queue_name):
        self._jconf.setYARNQueue(queue_name)
        return self

    def set_h2o_driver_if(self, ip):
        self._jconf.setH2ODriverIf(ip)
        return self

    def set_health_check_interval(self, interval):
        self._jconf.setHealthCheckInterval(interval)
        return self

    def set_kill_on_unhealthy_cluster_enabled(self):
        self._jconf.setKillOnUnhealthyClusterEnabled()
        return self

    def set_kill_on_unhealthy_cluster_disabled(self):
        self._jconf.setKillOnUnhealthyClusterDisabled()
        return self

    def set_kill_on_unhealthy_cluster_interval(self, interval):
        self._jconf.setKillOnUnhealthyClusterInterval(interval)
        return self

    def set_kerberos_principal(self, principal):
        self._jconf.setKerberosPrincipal(principal)
        return self

    def set_kerberos_keytab(self, path):
        self._jconf.setKerberosKeytab(path)
        return self

# getters independent on backend

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

    def hash_login(self):
        return self._jconf.hashLogin()

    def ldap_login(self):
        return self._jconf.ldapLogin()

    def kerberos_login(self):
        return self._jconf.kerberosLogin()

    def login_conf(self):
        return self._get_option(self._jconf.loginConf())

    def user_name(self):
        return self._get_option(self._jconf.userName())

    def ssl_conf(self):
        return self._get_option(self._jconf.sslConf())

    def auto_flow_ssl(self):
        return self._jconf.autoFlowSsl()

    def h2o_node_log_level(self):
        return self._jconf.h2oNodeLogLevel()

    def h2o_node_log_dir(self):
        return self._jconf.h2oNodeLogDir()

    def ui_update_interval(self):
        return self._jconf.uiUpdateInterval()

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

    # Getters for internal backend

    def use_flatfile(self):
        return self._jconf.useFlatFile()

    def ip_based_flatfile(self):
        return self._jconf.ipBasedFlatfile()

    def num_h2o_workers(self):
        return self._get_option(self._jconf.numH2OWorkers())

    def drdd_mul_factor(self):
        return self._jconf.drddMulFactor()

    def num_rdd_retries(self):
        return self._jconf.numRddRetries()

    def default_cloud_size(self):
        return self._jconf.defaultCloudSize()

    def subseq_tries(self):
        return self._jconf.subseqTries()

    def node_base_port(self):
        return self._jconf.nodeBasePort()

    def node_iced_dir(self):
        return self._get_option(self._jconf.nodeIcedDir())

    def is_internal_secure_connections_enabled(self):
        return self._jconf.isInternalSecureConnectionsEnabled()

    # Getters for external backend

    def h2o_cluster(self):
        return self._get_option(self._jconf.h2oCluster())

    def h2o_cluster_host(self):
        return self._get_option(self._jconf.h2oClusterHost())

    def h2o_cluster_port(self):
        return self._get_option(self._jconf.h2oClusterPort())

    def num_of_external_h2o_nodes(self):
        return self._get_option(self._jconf.numOfExternalH2ONodes())

    def client_check_retry_timeout(self):
        return self._jconf.clientCheckRetryTimeout()

    def client_connection_timeout(self):
        return self._jconf.clientConnectionTimeout()

    def external_read_confirmation_timeout(self):
        return self._jconf.externalReadConfirmationTimeout()

    def external_write_confirmation_timeout(self):
        return self._jconf.externalWriteConfirmationTimeout()

    def cluster_start_timeout(self):
        return self._jconf.clusterStartTimeout()

    def cluster_config_file(self):
        return self._get_option(self._jconf.clusterInfoFile())

    def mapper_xmx(self):
        return self._jconf.mapperXmx()

    def hdfs_output_dir(self):
        return self._get_option(self._jconf.HDFSOutputDir())

    def cluster_start_mode(self):
        return self._jconf.clusterStartMode()

    def is_auto_cluster_start_used(self):
        return self._jconf.isAutoClusterStartUsed()

    def is_manual_cluster_start_used(self):
        return self._jconf.isManualClusterStartUsed()

    def h2o_driver_path(self):
        return self._get_option(self._jconf.h2oDriverPath())

    def yarn_queue(self):
        return self._get_option(self._jconf.YARNQueue())

    def h2o_driver_if(self):
        return self._get_option(self._jconf.h2oDriverIf())

    def get_health_check_interval(self):
        return self._jconf.healthCheckInterval()

    def is_kill_on_unhealthy_cluster_enabled(self):
        return self._jconf.isKillOnUnhealthyClusterEnabled()

    def kill_on_unhealthy_cluster_interval(self):
        return self._jconf.killOnUnhealthyClusterInterval()

    def kerberos_principal(self):
        return self._get_option(self._jconf.kerberosPrincipal())

    def kerberos_keytab(self):
        return self._get_option(self._jconf.kerberosKeytab())

    def set(self, key, value):
        self._jconf.set(key, value)
        return self

    def remove(self, key):
        self._jconf.remove(key)
        return self

    def contains(self, key):
        return self._jconf.contains(key)

    def get(self, key, default_value=None):
        """
        Get a parameter, throws a NoSuchElementException if the value
        is not available and default_value not set
        """
        if default_value is None:
            return self._jconf.get(key)
        else:
            return self._jconf.get(key, default_value)

    def get_all(self):
        """
        Get all parameters as a list of pairs
        :return: list_of_configurations: List of pairs containing configurations
        """
        python_conf = []
        all = self._jconf.getAll()
        for conf in all:
            python_conf.append((conf._1(),conf._2()))
        return python_conf

    def set_all(self, list_of_configurations):
        """
        Set multiple parameters together
        :param list_of_configurations: List of pairs containing configurations
        :return: this H2O configuration
        """
        for conf in list_of_configurations:
            self._jconf.set(conf[0], conf[1])
        return self

    def __str__(self):
        return self._jconf.toString()

    def __repr__(self):
        self.show()
        return ""

    def show(self):
        print(self)
