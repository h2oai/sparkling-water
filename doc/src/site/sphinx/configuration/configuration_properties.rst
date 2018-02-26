.. _sw_config_properties:

Sparkling Water Configuration Properties
----------------------------------------

The following configuration properties can be passed to Spark to configure Sparking Water.

Configuration properties independent of selected backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------+----------------+----------------------------------------+
| Property name                                      | Default value  | Description                            |
+====================================================+================+========================================+
| **Generic parameters**                             |                |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.backend.cluster.mode``             | ``internal``   | This option can be set either to       |
|                                                    |                | ``internal`` or ``external``. When set |
|                                                    |                | to ``external``, ``H2O Context`` is    |
|                                                    |                | created by connecting to existing H2O  |
|                                                    |                | cluster, otherwise H2O cluster located |
|                                                    |                | inside Spark is created. That means    |
|                                                    |                | that each Spark executor will have one |
|                                                    |                | H2O instance running in it. The        |
|                                                    |                | ``internal`` mode is not recommended   |
|                                                    |                | for big clusters and clusters where    |
|                                                    |                | Spark executors are not stable.        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.cloud.name``                       | Generated      | Name of H2O cluster.                   |
|                                                    | unique name    |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.nthreads``                         | ``-1``         | Limit for number of threads used by    |
|                                                    |                | H2O, default ``-1`` means:             |
|                                                    |                | Use value of ``spark.executor.cores``  |
|                                                    |                | in case this property is set.          |
|                                                    |                | Otherwise use H2O's default value      |
|                                                    |                | |H2ONThreadsDefault|.                  |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.disable.ga``                       | ``true``       | Disable Google Analytics tracking for  |
|                                                    |                | embedded H2O.                          |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.repl.enabled``                     | ``true``       | Decides whether H2O REPL is initiated  |
|                                                    |                | or not.                                |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.scala.int.default.num``                | ``1``          | Number of parallel REPL sessions       |
|                                                    |                | started at the start of Sparkling      |
|                                                    |                | Water                                  |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.topology.change.listener.enabled`` | ``true``       | Decides whether listener which kills   |
|                                                    |                | H2O cluster on the change of the       |
|                                                    |                | underlying cluster's topology is       |
|                                                    |                | enabled or not. This configuration     |
|                                                    |                | has effect only in non-local mode.     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.spark.version.check.enabled``      | ``true``       | Enables check if run-time Spark        |
|                                                    |                | version matches build time Spark       |
|                                                    |                | version.                               |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.fail.on.unsupported.spark.param``  | ``true``       | If unsupported Spark parameter is      |
|                                                    |                | detected, then application is forced   |
|                                                    |                | to shutdown.                           |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.jks``                              | ``None``       | Path to Java KeyStore file.            |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.jks.pass``                         | ``None``       | Password for Java KeyStore file.       |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.hash.login``                       | ``false``      | Enable hash login.                     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.ldap.login``                       | ``false``      | Enable LDAP login.                     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.kerberos.login``                   | ``false``      | Enable Kerberos login.                 |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.login.conf``                       | ``None``       | Login configuration file.              |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.user.name``                        | ``None``       | Override user name for cluster.        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.internal_security_conf``           | ``None``       | Path to a file containing H2O or       |
|                                                    |                | Sparkling Water internal security      |
|                                                    |                | configuration.                         |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.log.level``                   | ``INFO``       | H2O internal log level used for H2O    |
|                                                    |                | nodes except the client.               |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.log.dir``                     | ``{user.dir}/h | Location of H2O logs on H2O nodes      |
|                                                    | 2ologs/{SparkA | except on the client.                  |
|                                                    | ppId}``        |                                        |
|                                                    | or YARN        |                                        |
|                                                    | container dir  |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.ui.update.interval``               | ``10000ms``    | Interval for updates of the Spark UI   |
|                                                    |                | and History server in milliseconds     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.cloud.timeout``                    | ``60*1000``    | Timeout (in msec) for cluster          |
|                                                    |                | formation.                             |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.enable.web``                  | ``false``      | Enable or disable web on H2O worker    |
|                                                    |                | nodes. It is disabled by default for   |
|                                                    |                | security reasons.                      |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.network.mask``                | ``None``       | Subnet selector for H2O running inside |
|                                                    |                | Spark executors. This disables using   |
|                                                    |                | IP reported by Spark but tries to find |
|                                                    |                | IP based on the specified mask.        |
+----------------------------------------------------+----------------+----------------------------------------+
| **H2O client parameters**                          |                |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.flow.dir``                  | ``None``       | Directory where flows from H2O Flow    |
|                                                    |                | are saved.                             |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.ip``                        | ``None``       | IP of H2O client node.                 |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.iced.dir``                  | ``None``       | Location of iced directory for the     |
|                                                    |                | driver instance.                       |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.log.level``                 | ``INFO``       | H2O internal log level used for H2O    |
|                                                    |                | client running inside Spark driver.    |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.log.dir``                   | ``{user.dir}/h | Location of H2O logs on the driver     |
|                                                    | 2ologs/{SparkA | machine.                               |
|                                                    | ppId}``        |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.port.base``                 | ``54321``      | Port on which H2O client publishes     |
|                                                    |                | its API. If already occupied, the next |
|                                                    |                | odd port is tried on so on.            |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.web.port``                  | ``-1``         | Exact client port to access web UI.    |
|                                                    |                | The value ``-1`` means automatic       |
|                                                    |                | search for a free port starting at     |
|                                                    |                | ``spark.ext.h2o.port.base``.           |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.verbose``                   | ``false``      | The client outputs verbose log output  |
|                                                    |                | directly into console. Enabling the    |
|                                                    |                | flag increases the client log level to |
|                                                    |                | ``INFO``.                              |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.client.network.mask``              | ``None``       | Subnet selector for H2O client, this   |
|                                                    |                | disables using IP reported by Spark    |
|                                                    |                | but tries to find IP based on the      |
|                                                    |                | specified mask.                        |
+----------------------------------------------------+----------------+----------------------------------------+

--------------

Internal backend configuration properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+----------------------------------------------------+----------------+----------------------------------------+
| Property name                                      | Default value  | Description                            |
+====================================================+================+========================================+
| **Generic parameters**                             |                |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.flatfile``                         | ``true``       | Use flatfile instead of multicast      |
|                                                    |                | approach for creating H2O cluster.     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.cluster.size``                     | ``None``       | Expected number of workers of H2O      |
|                                                    |                | cluster. Value None means automatic    |
|                                                    |                | detection of cluster size. This number |
|                                                    |                | must be equal to number of Spark       |
|                                                    |                | executors.                             |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.dummy.rdd.mul.factor``             | ``10``         | Multiplication factor for dummy RDD    |
|                                                    |                | generation. Size of dummy RDD is       |
|                                                    |                | ``spark.ext.h2o.cluster.size`` \*      |
|                                                    |                | ``spark.ext.h2o.dummy.rdd.mul.factor`` |
|                                                    |                | .                                      |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.spreadrdd.retries``                | ``10``         | Number of retries for creation of an   |
|                                                    |                | RDD spread across all existing Spark   |
|                                                    |                | executors.                             |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.default.cluster.size``             | ``20``         | Starting size of cluster in case that  |
|                                                    |                | size is not explicitly configured.     |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.subseq.tries``                     | ``5``          | Subsequent successful tries to figure  |
|                                                    |                | out size of Spark cluster, which are   |
|                                                    |                | producing the same number of nodes.    |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.internal_secure_connections``      | ``false``      | Enables secure communications among    |
|                                                    |                | H2O nodes. The security is based on    |
|                                                    |                | automatically generated keystore       |
|                                                    |                | and truststore. This is equivalent for |
|                                                    |                | ``-internal_secure_conections`` option |
|                                                    |                | in `H2O Hadoop deployments             |
|                                                    |                | <https://github.com/h2oai/h2o-3/blob/  |
|                                                    |                | master/h2o-docs/src/product/           |
|                                                    |                | security.rst#hadoop>`_.                |
+----------------------------------------------------+----------------+----------------------------------------+
| **H2O nodes parameters**                           |                |                                        |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.port.base``                   | ``54321``      | Base port used for individual H2O      |
|                                                    |                | nodes.                                 |
+----------------------------------------------------+----------------+----------------------------------------+
| ``spark.ext.h2o.node.iced.dir``                    | ``None``       | Location of iced directory for H2O     |
|                                                    |                | nodes on the Spark executors.          |
+----------------------------------------------------+----------------+----------------------------------------+

--------------

External backend configuration properties
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------------------------------------------+----------------+-------------------------------------+
| Property name                                         | Default value  | Description                         |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.cloud.representative``                | ``None``       | ip:port of arbitrary H2O node to    |
|                                                       |                | identify external H2O cluster.      |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.cluster.num.h2o.nodes``      | ``None``       | Number of H2O nodes to start in     |
|                                                       |                | ``auto`` mode and wait for in       |
|                                                       |                | ``manual`` mode when starting       |
|                                                       |                | Sparkling Water in external H2O     |
|                                                       |                | cluster mode.                       |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.cluster.client.retry.timeout``        | ``60000ms``    | Timeout in milliseconds specifying  |
|                                                       |                | how often the check for             |
|                                                       |                | availability of connected watchdog  |
|                                                       |                | client is done.                     |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.cluster.client.connect.timeout``      | ``180000ms``   | Timeout in milliseconds for         |
|                                                       |                | watchdog client connection. If the  |
|                                                       |                | client is not connected to the      |
|                                                       |                | external cluster in the given time  |
|                                                       |                | ,the cluster is killed.             |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.read.confirmation.timeout``  | ``60s``        | Timeout for confirmation of read    |
|                                                       |                | operation (H2O frame => Spark       |
|                                                       |                | frame) on external cluster.         |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.write.confirmation.timeout`` | ``60s``        | Timeout for confirmation of write   |
|                                                       |                | operation (Spark frame => H2O       |
|                                                       |                | frame) on external cluster.         |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.cluster.start.timeout``               | ``120s``       | Timeout in seconds for starting     |
|                                                       |                | H2O external cluster.               |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.cluster.info.name``                   | ``None``       | Full path to a file which is used   |
|                                                       |                | sd the notification file for the    |
|                                                       |                | startup of external H2O cluster.    |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.hadoop.memory``                       | ``6G``         | Amount of memory assigned to each   |
|                                                       |                | H2O node on YARN/Hadoop.            |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.hdfs.dir``                   | ``None``       | Path to the directory on HDFS used  |
|                                                       |                | for storing temporary files.        |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.start.mode``                 | ``manual``     | If this option is set to ``auto``   |
|                                                       |                | then H2O external cluster is        |
|                                                       |                | automatically started using the     |
|                                                       |                | provided H2O driver JAR on YARN,    |
|                                                       |                | otherwise it is expected that the   |
|                                                       |                | cluster is started by the user      |
|                                                       |                | manually.                           |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.h2o.driver``                 | ``None``       | Path to H2O driver used during      |
|                                                       |                | ``auto`` start mode.                |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.yarn.queue``                 | ``None``       | Yarn queue on which external H2O    |
|                                                       |                | cluster is started.                 |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.driver.if``                  | ``None``       | IP address of H2O driver in case of |
|                                                       |                | external cluster in automatic mode. |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.health.check.interval``      | ``HeartBeatThr | Health check interval for external  |
|                                                       | ead.TIMEOUT``  | H2O nodes.                          |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.kill.on.unhealthy``          | ``true``       | If true, the client will try to     |
|                                                       |                | kill the cluster and then itself in |
|                                                       |                | case some nodes in the cluster      |
|                                                       |                | report unhealthy status.            |
+-------------------------------------------------------+----------------+-------------------------------------+
| ``spark.ext.h2o.external.kill.on.unhealthy.interval`` | ``HeartBeatThr | How often check the healthy status  |
|                                                       | ead.TIMEOUT    | for the decision whether to kill    |
|                                                       | * 3``          | the cloud or not.                   |
+-------------------------------------------------------+----------------+-------------------------------------+

--------------

.. |H2ONThreadsDefault| replace:: ``Runtime.getRuntime().availableProcessors()``