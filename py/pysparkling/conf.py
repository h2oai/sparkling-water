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
        self._ss = spark_session
        self._sc = self._ss._sc
        jvm = self._sc._jvm
        jsc = self._sc._jsc
        # Create instance of H2OConf class
        self._jconf = jvm.org.apache.spark.h2o.H2OConf(jsc)

    def _get_option(self, option):
        if option.isDefined():
            return option.get()
        else:
            return None

    def runs_in_external_cluster_mode(self):
        return self._jconf.runsInExternalClusterMode()

    def runs_in_internal_cluster_mode(self):
        return self._jconf.runsInInternalClusterMode()

    # setters for most common properties
    # TODO: Create setters and getters for all properties
    def set_cloud_name(self, cloud_name):
        self._jconf.setCloudName(cloud_name)
        return self

    def set_num_of_external_h2o_nodes(self, num_of_external_h2o_nodes):
        self._jconf.setNumOfExternalH2ONodes(num_of_external_h2o_nodes)
        return self

    def set_internal_cluster_mode(self):
        self._jconf.setInternalClusterMode()
        return self

    def set_external_cluster_mode(self):
        self._jconf.setExternalClusterMode()
        return self

    def set_client_ip(self, ip):
        self._jconf.setClientIp(ip)
        return self

    def set_client_network_mask(self, mask):
        self._jconf.setClientNetworkMask(mask)
        return self

    def set_flatfile_path(self, flatfile_path):
        self._jconf.setFlatFilePath(flatfile_path)
        return self

    def set_h2o_cluster(self, ip, port):
        self._jconf.setH2OCluster(ip, port)
        return self

    def set_yarn_queue(self, queue_name):
        self._jconf.setYARNQueue(queue_name)
        return self

    def set_h2o_driver_path(self, driver_path):
        self._jconf.setH2ODriverPath(driver_path)
        return self

    def set_hdfs_output_dir(self, hdfs_output_dir):
        self._jconf.setHDFSOutputDir(hdfs_output_dir)
        return self

    def set_mapper_xmx(self, mem):
        self._jconf.setMapperXmx(mem)
        return self

    def set_cluster_config_file(self, path):
        self._jconf.setClusterConfigFile(path)
        return self

    def use_auto_cluster_start(self):
        self._jconf.useAutoClusterStart()
        return self

    def use_manual_cluster_start(self):
        self._jconf.useManualClusterStart()
        return self

    def set_h2o_node_log_level(self, level):
        self._jconf.setH2ONodeLogLevel(level)
        return self

    def set_cluster_start_timeout(self, timeout):
        """Set timeout for start of external cluster. If the cluster is not able to cloud within the timeout the
        the exception is thrown.

        Arguments:
        timeout -- timeout in seconds
        
        """
        self._jconf.setClusterStartTimeout(timeout)
        return self

    def set_h2o_client_log_level(self, level):
        self._jconf.setH2OClientLogLevel(level)
        return self

    def set_h2o_client_log_dir(self, log_dir):
        self._jconf.setH2OClientLogDir(log_dir)
        return self

    def set_client_connection_timeout(self, timeout):
        """Set timeout for watchdog client connection in external cluster mode. If the client is not connected to the
         cluster within the specified time, the cluster kill itself.

        Arguments:
        timeout -- timeout in milliseconds
        
        """
        self._jconf.setClientConnectionTimeout(timeout)
        return self

    def set_client_check_retry_timeout(self, timeout):
        """Set retry interval how often nodes in the external cluster mode check for the presence of the h2o client.
        
        Arguments:
        timeout -- timeout in milliseconds
        
        """
        self._jconf.setClientCheckRetryTimeout(timeout)
        return self

    def set_external_read_confirmation_timeout(self, timeout):
        self._jconf.setExternalReadConfirmationTimeout(timeout)
        return self

    def set_external_write_confirmation_timeout(self, timeout):
        self._jconf.setExternalWriteConfirmationTimeout(timeout)
        return self

    def set_ui_update_interval(self, interval):
        self._jconf.setUiUpdateInterval(interval)
        return self

    # getters

    def ui_update_interval(self):
        return self._jconf.uiUpdateInterval()

    def client_connection_timeout(self):
        return self._jconf.clientConnectionTimeout()

    def client_check_retry_timeout(self):
        return self._jconf.clientCheckRetryTimeout()

    def external_read_confirmation_timeout(self):
        return self._jconf.externalReadConfirmationTimeout()

    def external_write_confirmation_timeout(self):
        return self._jconf.externalWriteConfirmationTimeout()

    def cluster_start_timeout(self):
        return self._jconf.clusterStartTimeout()

    def h2o_cluster(self):
        return self._get_option(self._jconf.h2oCluster())

    def yarn_queue(self):
        return self._get_option(self._jconf.YARNQueue())

    def h2o_driver_path(self):
        return self._get_option(self._jconf.h2oDriverPath())

    def hdfs_output_dir(self):
        return self._get_option(self._jconf.HDFSOutputDir())

    def mapper_xmx(self):
        return self._jconf.mapperXmx()

    def cluster_config_file(self):
        return self._get_option(self._jconf.clusterInfoFile())

    def cluster_start_mode(self):
        return self._jconf.clusterStartMode()

    def is_auto_cluster_start_used(self):
        return self._jconf.isAutoClusterStartUsed()

    def is_manual_cluster_start_used(self):
        return self._jconf.isManualClusterStartUsed()

    def cloud_name(self):
        return self._get_option(self._jconf.cloudName())

    def num_of_external_h2o_nodes(self):
        return self._get_option(self._jconf.numOfExternalH2ONodes())

    def flatfile_path(self):
        return self._get_option(self._jconf.flatFilePath())

    def num_h2o_workers(self):
        return self._get_option(self._jconf.numH2OWorkers())

    def use_flatfile(self):
        return self._jconf.useFlatFile()

    def node_base_port(self):
        return self._jconf.nodeBasePort()

    def cloud_timeout(self):
        return self._jconf.cloudTimeout()

    def drdd_mul_factor(self):
        return self._jconf.drddMulFactor()

    def num_rdd_retries(self):
        return self._jconf.numRddRetries()

    def default_cloud_size(self):
        return self._jconf.defaultCloudSize()

    def h2o_node_log_level(self):
        return self._jconf.h2oNodeLogLevel()

    def h2o_node_log_dir(self):
        return self._jconf.h2oNodeLogDir()

    def node_iced_dir(self):
        return self._get_option(self._jconf.nodeIcedDir())

    def subseq_tries(self):
        return self._jconf.subseqTries()

    def backend_cluster_mode(self):
        return self._jconf.backendClusterMode()

    def client_ip(self):
        return self._get_option(self._jconf.clientIp())

    def client_base_port(self):
        return self._jconf.clientBasePort()

    def h2o_client_log_level(self):
        return self._jconf.h2oClientLogLevel()

    def h2o_client_log_dir(self):
        return self._get_option(self._jconf.h2oClientLogDir())

    def client_network_mask(self):
        return self._get_option(self._jconf.clientNetworkMask())

    def node_network_mask(self):
        return self._get_option(self._jconf.nodeNetworkMask())

    def nthreads(self):
        return self._jconf.nthreads()

    def disable_ga(self):
        return self._jconf.disableGA()

    def client_web_port(self):
        return self._jconf.clientWebPort()

    def client_iced_dir(self):
        return self._get_option(self._jconf.clientIcedDir())

    def jks(self):
        return self._get_option(self._jconf.jks())

    def jks_pass(self):
        return self._get_option(self._jconf.jksPass())

    def hash_login(self):
        return self._jconf.hashLogin()

    def ldap_login(self):
        return self._jconf.ldapLogin()

    def login_conf(self):
        return self._get_option(self._jconf.loginConf())

    def user_name(self):
        return self._get_option(self._jconf.userName())

    def scala_int_default_num(self):
        return self._jconf.scalaIntDefaultNum()

    def is_h2o_repl_enabled(self):
        return self._jconf.isH2OReplEnabled()

    def is_cluster_topology_listener_enabled(self):
        return self._jconf.isClusterTopologyListenerEnabled()

    def is_spark_version_check_enabled(self):
        return self._jconf.isSparkVersionCheckEnabled()

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
