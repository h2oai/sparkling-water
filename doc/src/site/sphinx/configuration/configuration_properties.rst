.. _sw_config_properties:

Sparkling Water Configuration Properties
----------------------------------------

The following configuration properties can be passed to Spark to configure Sparking Water.

Configuration properties independent of selected backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| Property name                                      | Default value  | H2OConf setter (* getter_)                      | Description                            |
+====================================================+================+=================================================+========================================+
| **Generic parameters**                             |                |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.backend.cluster.mode``             | ``internal``   | ``setInternalClusterMode()``                    | This option can be set either to       |
|                                                    |                |                                                 | ``internal`` or ``external``. When set |
|                                                    |                | ``setExternalClusterMode()``                    | to ``external``, ``H2O Context`` is    |
|                                                    |                |                                                 | created by connecting to existing H2O  |
|                                                    |                |                                                 | cluster, otherwise H2O cluster located |
|                                                    |                |                                                 | inside Spark is created. That means    |
|                                                    |                |                                                 | that each Spark executor will have one |
|                                                    |                |                                                 | H2O instance running in it. The        |
|                                                    |                |                                                 | ``internal`` mode is not recommended   |
|                                                    |                |                                                 | for big clusters and clusters where    |
|                                                    |                |                                                 | Spark executors are not stable.        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.cloud.name``                       | Generated      | ``setCloudName(String)``                        | Name of H2O cluster.                   |
|                                                    | unique name    |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.nthreads``                         | ``-1``         | ``setNthreads(Integer)``                        | Limit for number of threads used by    |
|                                                    |                |                                                 | H2O, default ``-1`` means:             |
|                                                    |                |                                                 | Use value of ``spark.executor.cores``  |
|                                                    |                |                                                 | in case this property is set.          |
|                                                    |                |                                                 | Otherwise use H2O's default value      |
|                                                    |                |                                                 | |H2ONThreadsDefault|.                  |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.repl.enabled``                     | ``true``       | ``setReplEnabled()``                            | Decides whether H2O REPL is initiated  |
|                                                    |                |                                                 | or not.                                |
|                                                    |                | ``setReplDisabled()``                           |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.scala.int.default.num``                | ``1``          | ``setDefaultNumReplSessions(Integer)``          | Number of parallel REPL sessions       |
|                                                    |                |                                                 | started at the start of Sparkling      |
|                                                    |                |                                                 | Water                                  |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.topology.change.listener.enabled`` | ``true``       | ``setClusterTopologyListenerEnabled()``         | Decides whether listener which kills   |
|                                                    |                |                                                 | H2O cluster on the change of the       |
|                                                    |                | ``setClusterTopologyListenerDisabled()``        | underlying cluster's topology is       |
|                                                    |                |                                                 | enabled or not. This configuration     |
|                                                    |                |                                                 | has effect only in non-local mode.     |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.spark.version.check.enabled``      | ``true``       | ``setSparkVersionCheckEnabled()``               | Enables check if run-time Spark        |
|                                                    |                |                                                 | version matches build time Spark       |
|                                                    |                | ``setSparkVersionCheckDisabled()``              | version.                               |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.fail.on.unsupported.spark.param``  | ``true``       | ``setFailOnUnsupportedSparkParamEnabled()``     | If unsupported Spark parameter is      |
|                                                    |                |                                                 | detected, then application is forced   |
|                                                    |                | ``setFailOnUnsupportedSparkParamDisabled()``    | to shutdown.                           |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.jks``                              | ``None``       | ``setJks(String)``                              | Path to Java KeyStore file.            |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.jks.pass``                         | ``None``       | ``setJksPass(String)``                          | Password for Java KeyStore file.       |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.jks.alias``                        | ``None``       | ``setJksAlias(String)``                         | Alias to certificate in keystore to    |
|                                                    |                |                                                 | secure H2O Flow.                       |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.hash.login``                       | ``false``      | ``setHashLoginEnabled()``                       | Enable hash login.                     |
|                                                    |                |                                                 |                                        |
|                                                    |                | ``setHashLoginDisabled()``                      |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.ldap.login``                       | ``false``      | ``setLdapLoginEnabled()``                       | Enable LDAP login.                     |
|                                                    |                |                                                 |                                        |
|                                                    |                | ``setLdapLoginDisabled()``                      |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.kerberos.login``                   | ``false``      | ``setKerberosLoginEnabled()``                   | Enable Kerberos login.                 |
|                                                    |                |                                                 |                                        |
|                                                    |                | ``setKerberosLoginDisabled()``                  |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.login.conf``                       | ``None``       | ``setLoginConf(String)``                        | Login configuration file.              |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.user.name``                        | ``None``       | ``setUserName(String)``                         | Username used for the backend H2O      |
|                                                    |                |                                                 | cluster and to authenticate the        |
|                                                    |                |                                                 | client against the backend.            |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.password``                         | ``None``       | ``setPassword(String)``                         | Password used to authenticate the      |
|                                                    |                |                                                 | client against the backend.            |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.internal_security_conf``           | ``None``       | ``setSslConf(String)``                          | Path to a file containing H2O or       |
|                                                    |                |                                                 | Sparkling Water internal security      |
|                                                    |                |                                                 | configuration.                         |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.auto.flow.ssl``                    | ``false``      | ``setAutoFlowSslEnabled()``                     | Automatically generate the required    |
|                                                    |                |                                                 | key store and password to secure H2O   |
|                                                    |                | ``setAutoFlowSslDisabled()``                    | flow by SSL.                           |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.log.level``                   | ``INFO``       | ``setH2ONodeLogLevel(String)``                  | H2O internal log level used for H2O    |
|                                                    |                |                                                 | nodes except the client.               |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.log.dir``                     | |h2oLogDir|    | ``setH2ONodeLogDir(String)``                    | Location of H2O logs on H2O nodes      |
|                                                    |                |                                                 | except on the client.                  |
|                                                    | or |yarnDir|   |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.backend.heartbeat.interval``       | ``10000ms``    | ``setBackendHeartbeatInterval(Integer)``        | Interval for getting heartbeat from    |
|                                                    |                |                                                 | the H2O backend.                       |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.cloud.timeout``                    | ``60*1000``    | ``setCloudTimeout(Integer)``                    | Timeout (in msec) for cluster          |
|                                                    |                |                                                 | formation.                             |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.network.mask``                | ``None``       | ``setNodeNetworkMask(String)``                  | Subnet selector for H2O running inside |
|                                                    |                |                                                 | Spark executors. This disables using   |
|                                                    |                |                                                 | IP reported by Spark but tries to find |
|                                                    |                |                                                 | IP based on the specified mask.        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.stacktrace.collector.interval``    | ``-1``         | ``setStacktraceCollectorInterval(Integer)``     | Interval specifying how often stack    |
|                                                    |                |                                                 | traces are taken on each H2O node.     |
|                                                    |                |                                                 | -1 means that no stack traces will be  |
|                                                    |                |                                                 | taken.                                 |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.context.path``                     | ``None``       | ``setContextPath(String)``                      | Context path to expose H2O web server. |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.flow.scala.cell.async``            | ``false``      | ``setFlowScalaCellAsyncEnabled()``              | Decide whether the Scala cells in      |
|                                                    |                |                                                 | H2O Flow will run synchronously or     |
|                                                    |                | ``setFlowScalaCellAsyncDisabled()``             | Asynchronously. Default is             |
|                                                    |                |                                                 | synchronously.                         |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.flow.scala.cell.max.parallel``     | ``-1``         | ``setMaxParallelScalaCellJobs(Integer)``        | Number of max parallel Scala cell      |
|                                                    |                |                                                 | jobs The value -1 means                |
|                                                    |                |                                                 | not limited.                           |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.internal.port.offset``             | ``1``          | ``setInternalPortOffset(Integer)``              | Offset between the API(=web) port and  |
|                                                    |                |                                                 | the internal communication port on the |
|                                                    |                |                                                 | client node;                           |
|                                                    |                |                                                 | ``api_port + port_offset = h2o_port``  |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.port.base``                   | ``54321``      | ``setNodeBasePort(Integer)``                    | Base port used for individual H2O      |
|                                                    |                |                                                 | nodes.                                 |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.mojo.destroy.timeout``             | ``600000``     | ``setMojoDestroyTimeout(Integer)``              | If a scoring MOJO instance is not used |
|                                                    |                |                                                 | within a Spark executor JVM for        |
|                                                    |                |                                                 | a given timeout in milliseconds, it's  |
|                                                    |                |                                                 | evicted from executor's cache. Default |
|                                                    |                |                                                 | timeout value is 10 minutes.           |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.extra``                       | ``None``       | ``setNodeExtraProperties(String)``              | A string containing extra parameters   |
|                                                    |                |                                                 | passed to H2O nodes during startup.    |
|                                                    |                |                                                 | This parameter should be configured    |
|                                                    |                |                                                 | only if H2O parameters do not have any |
|                                                    |                |                                                 | corresponding parameters in Sparkling  |
|                                                    |                |                                                 | Water.                                 |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.flow.extra.http.headers``          | ``None``       | ``setFlowExtraHttpHeaders(Map[String,String])`` | Extra HTTP headers that will be used   |
|                                                    |                |                                                 | in communication between the front-end |
|                                                    |                | ``setFlowExtraHttpHeaders(String)``             | and back-end part of Flow UI.          |
|                                                    |                |                                                 | The headers should be delimited by     |
|                                                    |                |                                                 | a new line. Don't forget to escape     |
|                                                    |                |                                                 | special characters when passing        |
|                                                    |                |                                                 | the parameter from a command line.     |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.internal_secure_connections``      | ``false``      | ``setInternalSecureConnectionsEnabled()``       | Enables secure communications among    |
|                                                    |                |                                                 | H2O nodes. The security is based on    |
|                                                    |                | ``setInternalSecureConnectionsDisabled()``      | automatically generated keystore       |
|                                                    |                |                                                 | and truststore. This is equivalent for |
|                                                    |                |                                                 | ``-internal_secure_conections`` option |
|                                                    |                |                                                 | in `H2O Hadoop deployments             |
|                                                    |                |                                                 | <https://github.com/h2oai/h2o-3/blob/  |
|                                                    |                |                                                 | master/h2o-docs/src/product/           |
|                                                    |                |                                                 | security.rst#hadoop>`_.                |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.follow_spark_time_zone``           | ``true``       | ``setSparkTimeZoneFollowingEnabled()``          | Determines whether H2O cluster will    |
|                                                    |                |                                                 | use the same timezone settings as      |
|                                                    |                | ``setSparkTimeZoneFollowingDisabled()``         | the running Spark session              |
|                                                    |                |                                                 | (``spark.sql.session.timeZone``).      |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| **H2O client parameters**                          |                |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.flow.dir``                  | ``None``       | ``setFlowDir(String)``                          | Directory where flows from H2O Flow    |
|                                                    |                |                                                 | are saved.                             |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.ip``                        | ``None``       | ``setClientIp(String)``                         | IP of H2O client node.                 |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.iced.dir``                  | ``None``       | ``setClientIcedDir(String)``                    | Location of iced directory for the     |
|                                                    |                |                                                 | driver instance.                       |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.log.level``                 | ``INFO``       | ``setH2OClientLogLevel(String)``                | H2O internal log level used for H2O    |
|                                                    |                |                                                 | client running inside Spark driver.    |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.log.dir``                   | |h2oLogDir|    | ``setH2OClientLogDir(String)``                  | Location of H2O logs on the driver     |
|                                                    |                |                                                 | machine.                               |
|                                                    |                |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.port.base``                 | ``54321``      | ``setClientBasePort(Integer)``                  | Port on which H2O client publishes     |
|                                                    |                |                                                 | its API. If already occupied, the next |
|                                                    |                |                                                 | odd port is tried on so on.            |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.web.port``                  | ``-1``         | ``setClientWebPort(Integer)``                   | Exact client port to access web UI.    |
|                                                    |                |                                                 | The value ``-1`` means automatic       |
|                                                    |                |                                                 | search for a free port starting at     |
|                                                    |                |                                                 | ``spark.ext.h2o.port.base``.           |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.verbose``                   | ``false``      | ``setClientVerboseEnabled()``                   | The client outputs verbose log output  |
|                                                    |                |                                                 | directly into console. Enabling the    |
|                                                    |                | ``setClientVerboseDisabled()``                  | flag increases the client log level to |
|                                                    |                |                                                 | ``INFO``.                              |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.network.mask``              | ``None``       | ``setClientNetworkMask(String)``                | Subnet selector for H2O client, this   |
|                                                    |                |                                                 | disables using IP reported by Spark    |
|                                                    |                |                                                 | but tries to find IP based on the      |
|                                                    |                |                                                 | specified mask.                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.flow.baseurl.override``     | ``None``       | ``setClientFlowBaseurlOverride(String)``        | Allows to override the base URL        |
|                                                    |                |                                                 | address of Flow UI, including the      |
|                                                    |                |                                                 | scheme, which is showed to the user.   |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.cluster.client.retry.timeout``     | ``60000``      | ``setClientCheckRetryTimeout(Integer)``         | Timeout in milliseconds specifying     |
|                                                    |                |                                                 | how often we check whether the         |
|                                                    |                |                                                 | the client is still connected.         |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.client.extra``                     | ``None``       | ``setClientExtraProperties(String)``            | A string containing extra parameters   |
|                                                    |                |                                                 | passed to H2O client during startup.   |
|                                                    |                |                                                 | This parameter should be configured    |
|                                                    |                |                                                 | only if H2O parameters do not have any |
|                                                    |                |                                                 | corresponding parameters in Sparkling  |
|                                                    |                |                                                 | Water.                                 |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.verify_ssl_certificates``          | ``True``       | ``setVerifySslCertificates(Boolean)``           | Whether certificates should be         |
|                                                    |                |                                                 | verified before using in H2O or not.   |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+

--------------

Internal backend configuration properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| Property name                                      | Default value  | H2OConf setter (* getter_)                      | Description                            |
+====================================================+================+=================================================+========================================+
| **Generic parameters**                             |                |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.cluster.size``                     | ``None``       | ``setNumH2OWorkers(Integer)``                   | Expected number of workers of H2O      |
|                                                    |                |                                                 | cluster. Value None means automatic    |
|                                                    |                |                                                 | detection of cluster size. This number |
|                                                    |                |                                                 | must be equal to number of Spark       |
|                                                    |                |                                                 | executors.                             |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.dummy.rdd.mul.factor``             | ``10``         | ``setDrddMulFactor(Integer)``                   | Multiplication factor for dummy RDD    |
|                                                    |                |                                                 | generation. Size of dummy RDD is       |
|                                                    |                |                                                 | ``spark.ext.h2o.cluster.size`` \*      |
|                                                    |                |                                                 | ``spark.ext.h2o.dummy.rdd.mul.factor`` |
|                                                    |                |                                                 | .                                      |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.spreadrdd.retries``                | ``10``         | ``setNumRddRetries(Integer)``                   | Number of retries for creation of an   |
|                                                    |                |                                                 | RDD spread across all existing Spark   |
|                                                    |                |                                                 | executors.                             |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.default.cluster.size``             | ``20``         | ``setDefaultCloudSize(Integer)``                | Starting size of cluster in case that  |
|                                                    |                |                                                 | size is not explicitly configured.     |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.subseq.tries``                     | ``5``          | ``setSubseqTries(Integer)``                     | Subsequent successful tries to figure  |
|                                                    |                |                                                 | out size of Spark cluster, which are   |
|                                                    |                |                                                 | producing the same number of nodes.    |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.hdfs_conf``                        | |hadoopConfig| | ``setHdfsConf(String)``                         | Either a string with the Path to a file|
|                                                    |                |                                                 | with Hadoop HDFS configuration or the  |
|                                                    |                |                                                 | org.apache.hadoop.conf.Configuration   |
|                                                    |                |                                                 | object. Useful for HDFS credentials    |
|                                                    |                |                                                 | settings and other HDFS-related        |
|                                                    |                |                                                 | configurations.                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| **H2O nodes parameters**                           |                |                                                 |                                        |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+
| ``spark.ext.h2o.node.iced.dir``                    | ``None``       | ``setNodeIcedDir(String)``                      | Location of iced directory for H2O     |
|                                                    |                |                                                 | nodes on the Spark executors.          |
+----------------------------------------------------+----------------+-------------------------------------------------+----------------------------------------+

--------------

External backend configuration properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| Property name                                         | Default value  | H2OConf setter (* getter_)                      | Description                         |
+=======================================================+================+=================================================+=====================================+
| ``spark.ext.h2o.cloud.representative``                | ``None``       | ``setH2OCluster(String)``                       | ip:port of arbitrary H2O node to    |
|                                                       |                |                                                 | identify external H2O cluster.      |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.cluster.size``               | ``None``       | ``setClusterSize(Integer)``                     | Number of H2O nodes to start in     |
|                                                       |                |                                                 | ``auto`` mode and wait for in       |
|                                                       |                |                                                 | ``manual`` mode when starting       |
|                                                       |                |                                                 | Sparkling Water in external H2O     |
|                                                       |                |                                                 | cluster mode.                       |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.cluster.start.timeout``               | ``120s``       | ``setClusterStartTimeout(Integer)``             | Timeout in seconds for starting     |
|                                                       |                |                                                 | H2O external cluster.               |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.cluster.info.name``                   | ``None``       | ``setClusterInfoFile(Integer)``                 | Full path to a file which is used   |
|                                                       |                |                                                 | sd the notification file for the    |
|                                                       |                |                                                 | startup of external H2O cluster.    |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.hadoop.memory``                       | ``6G``         | ``setMapperXmx(String)``                        | Amount of memory assigned to each   |
|                                                       |                |                                                 | H2O node on YARN/Hadoop.            |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.hdfs.dir``                   | ``None``       | ``setHDFSOutputDir(String)``                    | Path to the directory on HDFS used  |
|                                                       |                |                                                 | for storing temporary files.        |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.start.mode``                 | ``manual``     | ``useAutoClusterStart()``                       | If this option is set to ``auto``   |
|                                                       |                |                                                 | then H2O external cluster is        |
|                                                       |                | ``useManualClusterStart()``                     | automatically started using the     |
|                                                       |                |                                                 | provided H2O driver JAR on YARN,    |
|                                                       |                |                                                 | otherwise it is expected that the   |
|                                                       |                |                                                 | cluster is started by the user      |
|                                                       |                |                                                 | manually.                           |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.h2o.driver``                 | ``None``       | ``setH2ODriverPath(String)``                    | Path to H2O driver used during      |
|                                                       |                |                                                 | ``auto`` start mode.                |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.yarn.queue``                 | ``None``       | ``setYARNQueue(String)``                        | Yarn queue on which external H2O    |
|                                                       |                |                                                 | cluster is started.                 |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.kill.on.unhealthy``          | ``true``       | ``setKillOnUnhealthyClusterEnabled()``          | If true, the client will try to     |
|                                                       |                |                                                 | kill the cluster and then itself in |
|                                                       |                | ``setKillOnUnhealthyClusterDisabled()``         | case some nodes in the cluster      |
|                                                       |                |                                                 | report unhealthy status.            |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.kerberos.principal``         | ``None``       | ``setKerberosPrincipal(String)``                | Kerberos Principal.                 |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.kerberos.keytab``            | ``None``       | ``setKerberosKeytab(String)``                   | Kerberos Keytab.                    |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.run.as.user``                | ``None``       | ``setRunAsUser(String)``                        | Impersonated Hadoop user.           |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.driver.if``                  | ``None``       | ``setExternalH2ODriverIf(String)``              | Ip address or network of            |
|                                                       |                |                                                 | mapper->driver callback interface.  |
|                                                       |                |                                                 | Default value means automatic       |
|                                                       |                |                                                 | detection.                          |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.driver.port``                | ``None``       | ``setExternalH2ODriverPort(Integer)``           | Port of mapper->driver callback     |
|                                                       |                |                                                 | interface. Default value means      |
|                                                       |                |                                                 | automatic detection.                |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.driver.port.range``          | ``None``       | ``setExternalH2ODriverPortRange(String)``       | Range portX-portY of mapper->driver |
|                                                       |                |                                                 | callback interface; eg:             |
|                                                       |                |                                                 | 50000-55000.                        |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.extra.memory.percent``       | ``10``         | ``setExternalExtraMemoryPercent(Integer)``      | This option is a percentage of      |
|                                                       |                |                                                 | ``spark.ext.h2o.hadoop.memory`` and |
|                                                       |                |                                                 | specifies memory for internal JVM   |
|                                                       |                |                                                 | use outside of Java heap.           |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.backend.stop.timeout``       | ``10000ms``    | ``setExternalBackendStopTimeout(Integer)``      | Timeout for confirmation from       |
|                                                       |                |                                                 | worker nodes when stopping the      |
|                                                       |                |                                                 | external backend. It is also        |
|                                                       |                |                                                 | possible to pass ``-1`` to ensure   |
|                                                       |                |                                                 | the indefinite timeout. The unit is |
|                                                       |                |                                                 | milliseconds.                       |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.hadoop.executable``          | ``hadoop``     | ``setExternalHadoopExecutable(String)``         | Name or path to path to a hadoop    |
|                                                       |                |                                                 | executable binary which is used     |
|                                                       |                |                                                 | to start external H2O backend on    |
|                                                       |                |                                                 | YARN.                               |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.extra.jars``                 | ``None``       | ``setExternalExtraJars(String)``                | Comma-separated paths to jars that  |
|                                                       |                |                                                 | will be placed onto classpath of    |
|                                                       |                | ``setExternalExtraJars(String[])``              | each H2O node.                      |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+
| ``spark.ext.h2o.external.communication.compression``  | ``SNAPPY``     | ``setExternalCommunicationCompression(String)`` | The type of compression used for    |
|                                                       |                |                                                 | data transfer between Spark and H2O |
|                                                       |                |                                                 | node. Possible values are ``NONE``, |
|                                                       |                |                                                 | ``DEFLATE``, ``GZIP``, ``SNAPPY``.  |
+-------------------------------------------------------+----------------+-------------------------------------------------+-------------------------------------+

.. _getter:

H2OConf getter can be derived from the corresponding setter. All getters are parameter-less. If the type of the property is Boolean, the getter is prefixed with
``is`` (E.g. ``setReplEnabled()`` -> ``isReplEnabled()``). Property getters of other types do not have any prefix and start with lowercase
(E.g. ``setUserName(String)`` -> ``userName`` for Scala, ``userName()`` for Python).


.. |H2ONThreadsDefault| replace:: ``Runtime.getRuntime().availableProcessors()``
.. |hadoopConfig| replace:: ``sc.hadoopConfig``
.. |h2oLogDir| replace:: ``{user.dir}/h2ologs/{SparkAppId}``
.. |yarnDir| replace:: YARN container dir