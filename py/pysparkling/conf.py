
from pysparkling.initializer import Initializer

class H2OConf(object):
    def __init__(self, spark_context):
        try:
            Initializer.load_sparkling_jar(spark_context)
            self._do_init(spark_context)
        except:
            raise


    def _do_init(self, spark_context):
        self._sc = spark_context
        jvm = self._sc._jvm
        gw = self._sc._gateway
        jsc = self._sc._jsc
        conf_klazz = jvm.java.lang.Thread.currentThread().getContextClassLoader().loadClass("org.apache.spark.h2o.H2OConf")
        # Find constructor with right arguments
        constructor_def = gw.new_array(jvm.Class, 1)
        constructor_def[0] = jsc.getClass()
        jconf_constructor = conf_klazz.getConstructor(constructor_def)
        constructor_params = gw.new_array(jvm.Object, 1)
        constructor_params[0] = jsc
        # Create instance of H2OConf class
        self._jconf = jconf_constructor.newInstance(constructor_params)

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
        self._jconf.setClientIP(ip)
        return self

    def set_client_network_mask(self, mask):
        self._jconf.setClientNetworkMask(mask)
        return self

    def set_flatfile_path(self, flatfile_path):
        self._jconf.setFlatFilePath(flatfile_path)
        return self

    def set_h2o_cloud(self, ip, port):
        self._jconf.setH2OCluster(ip, port)
        return self
    
    # getters
    def cloud_name(self):
        return self._get_option(self._jconf.cloudName())


    def num_of_external_h2o_nodes(self):
        return self._get_option(self._jconf.numOfExternalH2ONodes())

    def flatfile_path(self):
        return self._get_option(self._jconf.flatFilePath())

    def num_H2O_Workers(self):
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
        return self._jconf.h2oClientLogDir()

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


    def get(self, key):
        """
        Get a parameter; throws a NoSuchElementException if it's not set
        """
        return self._jconf.get(key)


    def get(self, key, defaultValue):
        """
        Get a parameter, falling back to a default if not set
        """
        return self._jconf.get(key, defaultValue)



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
        print self
