Migration Guide
===============

Migration guide between Sparkling Water versions.

From 3.28.1 to 3.30
-------------------

Removal of Deprecated Methods and Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- On PySparkling, passing authentication on ``H2OContext`` via ``auth`` param is removed in favor of methods
  ``setUserName`` and ``setPassword`` ond the ``H2OConf`` or via
  the Spark options ``spark.ext.h2o.user.name`` and ``spark.ext.h2o.password`` directly.

- On Pysparkling, passing ``verify_ssl_certificates`` parameter as H2OContext argument is removed in favor of
  method ``setVerifySslCertificates`` on ``H2OConf`` or via the spark option ``spark.ext.h2o.verify_ssl_certificates``.

- On RSparkling, the method ``h2o_context`` is removed. To create H2OContext, please call
  ``hc <- H2OContext.getOrCreate(sc)``. Also the methods ``h2o_flow``, ``as_h2o_frame`` and ``as_spark_dataframe`` are
  removed. Please use the methods available on the ``H2OContext`` instance created via ``hc <- H2OContext.getOrCreate(sc)``.
  Instead of ``h2o_flow``, use ``hc$openFlow``, instead of ``as_h2o_frame``, use ``asH2OFrame`` and instead of
  ``as_spark_dataframe`` use ``asSparkFrame``.

  Also the ``H2OContext.getOrCreate(sc)`` does not have ``username`` and ``password`` arguments anymore.
  The correct way how to pass authentication details to ``H2OContext`` is via ``H2OConf`` class, such as:

  .. code-block:: r

    conf <- H2OConf(sc)
    conf$setUserName(username)
    conf$setPassword(password)
    hc <- H2OContext.getOrCreate(sc, conf)

  The Spark options ``spark.ext.h2o.user.name`` and ``spark.ext.h2o.password`` correspond to these setters and can be
  also used directly.

- In ``H2OContext`` Python API, the method ``as_spark_frame`` is replaced by the method ``asSparkFrame`` and the method
  ``as_h2o_frame`` is replaced by ``asH2OFrame``.

- In ``H2OXGBoost`` Scala And Python API, the methods ``getNEstimators`` and ``setNEstimators`` are removed. Please use ``getNtrees`` and
  ``setNtrees`` instead.

- The default value of ``spark.ext.h2o.internal_secure_connections`` has changed to ``true`` which means that Sparkling Water
  in internal backend and automatic mode of external backend is now running secured by default.

- In Scala and Python API for tree-based algorithms, the method ``getR2Stopping`` is removed in favor of ``getStoppingRounds``,
  ``getStoppingMetric``, ``getStoppingTolerance`` methods and the method ``setR2Stopping`` is removed in favor of
  ``setStoppingRounds``, ``setStoppingMetric``, ``setStoppingTolerance`` methods.

- On H2OConf Python API, the following methods have been renamed to be consistent with the Scala counterparts:

       - ``h2o_cluster`` -> ``h2oCluster``
       - ``h2o_cluster_host`` -> ``h2oClusterHost``
       - ``h2o_cluster_port`` -> ``h2oClusterPort``
       - ``cluster_size`` -> ``clusterSize``
       - ``cluster_start_timeout`` -> ``clusterStartTimeout``
       - ``cluster_config_file`` -> ``clusterInfoFile``
       - ``mapper_xmx`` -> ``mapperXmx``
       - ``hdfs_output_dir`` -> ``HDFSOutputDir``
       - ``cluster_start_mode`` -> ``clusterStartMode``
       - ``is_auto_cluster_start_used`` -> ``isAutoClusterStartUsed``
       - ``is_manual_cluster_start_used`` -> ``isManualClusterStartUsed``
       - ``h2o_driver_path`` -> ``h2oDriverPath``
       - ``yarn_queue`` -> ``YARNQueue``
       - ``is_kill_on_unhealthy_cluster_enabled`` -> ``isKillOnUnhealthyClusterEnabled``
       - ``kerberos_principal`` -> ``kerberosPrincipal``
       - ``kerberos_keytab`` -> ``kerberosKeytab``
       - ``run_as_user`` -> ``runAsUser``
       - ``set_h2o_cluster`` -> ``setH2OCluster``
       - ``set_cluster_size`` -> ``setClusterSize``
       - ``set_cluster_start_timeout`` -> ``setClusterStartTimeout``
       - ``set_cluster_config_file`` -> ``setClusterConfigFile``
       - ``set_mapper_xmx`` -> ``setMapperXmx``
       - ``set_hdfs_output_dir`` -> ``setHDFSOutputDir``
       - ``use_auto_cluster_start`` -> ``useAutoClusterStart``
       - ``use_manual_cluster_start`` -> ``useManualClusterStart``
       - ``set_h2o_driver_path`` -> ``setH2ODriverPath``
       - ``set_yarn_queue`` -> ``setYARNQueue``
       - ``set_kill_on_unhealthy_cluster_enabled`` -> ``setKillOnUnhealthyClusterEnabled``
       - ``set_kill_on_unhealthy_cluster_disabled`` -> ``setKillOnUnhealthyClusterDisabled``
       - ``set_kerberos_principal`` -> ``setKerberosPrincipal``
       - ``set_kerberos_keytab`` -> ``setKerberosKeytab``
       - ``set_run_as_user`` -> ``setRunAsUser``
       - ``num_h2o_workers`` -> ``numH2OWorkers``
       - ``drdd_mul_factor`` -> ``drddMulFactor``
       - ``num_rdd_retries`` -> ``numRddRetries``
       - ``default_cloud_size`` -> ``defaultCloudSize``
       - ``subseq_tries`` -> ``subseqTries``
       - ``h2o_node_web_enabled`` -> ``h2oNodeWebEnabled``
       - ``node_iced_dir`` -> ``nodeIcedDir``
       - ``set_num_h2o_workers`` -> ``setNumH2OWorkers``
       - ``set_drdd_mul_factor`` -> ``setDrddMulFactor``
       - ``set_num_rdd_retries`` -> ``setNumRddRetries``
       - ``set_default_cloud_size`` -> ``setDefaultCloudSize``
       - ``set_subseq_tries`` -> ``setSubseqTries``
       - ``set_h2o_node_web_enabled`` -> ``setH2ONodeWebEnabled``
       - ``set_h2o_node_web_disabled`` -> ``setH2ONodeWebDisabled``
       - ``set_node_iced_dir`` -> ``setNodeIcedDir``
       - ``backend_cluster_mode`` -> ``backendClusterMode``
       - ``cloud_name`` -> ``cloudName``
       - ``is_h2o_repl_enabled`` -> ``isH2OReplEnabled``
       - ``scala_int_default_num`` -> ``scalaIntDefaultNum``
       - ``is_cluster_topology_listener_enabled`` -> ``isClusterTopologyListenerEnabled``
       - ``is_spark_version_check_enabled`` -> ``isSparkVersionCheckEnabled``
       - ``is_fail_on_unsupported_spark_param_enabled`` -> ``isFailOnUnsupportedSparkParamEnabled``
       - ``jks_pass`` -> ``jksPass``
       - ``jks_alias`` -> ``jksAlias``
       - ``hash_login`` -> ``hashLogin``
       - ``ldap_login`` -> ``ldapLogin``
       - ``kerberos_login`` -> ``kerberosLogin``
       - ``login_conf`` -> ``loginConf``
       - ``ssl_conf`` -> ``sslConf``
       - ``auto_flow_ssl`` -> ``autoFlowSsl``
       - ``h2o_node_log_level`` -> ``h2oNodeLogLevel``
       - ``h2o_node_log_dir`` -> ``h2oNodeLogDir``
       - ``cloud_timeout`` -> ``cloudTimeout``
       - ``node_network_mask`` -> ``nodeNetworkMask``
       - ``stacktrace_collector_interval`` -> ``stacktraceCollectorInterval``
       - ``context_path`` -> ``contextPath``
       - ``flow_scala_cell_async`` -> ``flowScalaCellAsync``
       - ``max_parallel_scala_cell_jobs`` -> ``maxParallelScalaCellJobs``
       - ``internal_port_offset`` -> ``internalPortOffset``
       - ``mojo_destroy_timeout`` -> ``mojoDestroyTimeout``
       - ``node_base_port`` -> ``nodeBasePort``
       - ``node_extra_properties`` -> ``nodeExtraProperties``
       - ``flow_extra_http_headers`` -> ``flowExtraHttpHeaders``
       - ``is_internal_secure_connections_enabled`` -> ``isInternalSecureConnectionsEnabled``
       - ``flow_dir`` -> ``flowDir``
       - ``client_ip`` -> ``clientIp``
       - ``client_iced_dir`` -> ``clientIcedDir``
       - ``h2o_client_log_level`` -> ``h2oClientLogLevel``
       - ``h2o_client_log_dir`` -> ``h2oClientLogDir``
       - ``client_base_port`` -> ``clientBasePort``
       - ``client_web_port`` -> ``clientWebPort``
       - ``client_verbose_output`` -> ``clientVerboseOutput``
       - ``client_network_mask`` -> ``clientNetworkMask``
       - ``ignore_spark_public_dns`` -> ``ignoreSparkPublicDNS``
       - ``client_web_enabled`` -> ``clientWebEnabled``
       - ``client_flow_baseurl_override`` -> ``clientFlowBaseurlOverride``
       - ``client_extra_properties`` -> ``clientExtraProperties``
       - ``runs_in_external_cluster_mode`` -> ``runsInExternalClusterMode``
       - ``runs_in_internal_cluster_mode`` -> ``runsInInternalClusterMode``
       - ``client_check_retry_timeout`` -> ``clientCheckRetryTimeout``
       - ``set_internal_cluster_mode`` -> ``setInternalClusterMode``
       - ``set_external_cluster_mode`` -> ``setExternalClusterMode``
       - ``set_cloud_name`` -> ``setCloudName``
       - ``set_nthreads`` -> ``setNthreads``
       - ``set_repl_enabled`` -> ``setReplEnabled``
       - ``set_repl_disabled`` -> ``setReplDisabled``
       - ``set_default_num_repl_sessions`` -> ``setDefaultNumReplSessions``
       - ``set_cluster_topology_listener_enabled`` -> ``setClusterTopologyListenerEnabled``
       - ``set_cluster_topology_listener_disabled`` -> ``setClusterTopologyListenerDisabled``
       - ``set_spark_version_check_disabled`` -> ``setSparkVersionCheckDisabled``
       - ``set_fail_on_unsupported_spark_param_enabled`` -> ``setFailOnUnsupportedSparkParamEnabled``
       - ``set_fail_on_unsupported_spark_param_disabled`` -> ``setFailOnUnsupportedSparkParamDisabled``
       - ``set_jks`` -> ``setJks``
       - ``set_jks_pass`` -> ``setJksPass``
       - ``set_jks_alias`` -> ``setJksAlias``
       - ``set_hash_login_enabled`` -> ``setHashLoginEnabled``
       - ``set_hash_login_disabled`` -> ``setHashLoginDisabled``
       - ``set_ldap_login_enabled`` -> ``setLdapLoginEnabled``
       - ``set_ldap_login_disabled`` -> ``setLdapLoginDisabled``
       - ``set_kerberos_login_enabled`` -> ``setKerberosLoginEnabled``
       - ``set_kerberos_login_disabled`` -> ``setKerberosLoginDisabled``
       - ``set_login_conf`` -> ``setLoginConf``
       - ``set_ssl_conf`` -> ``setSslConf``
       - ``set_auto_flow_ssl_enabled`` -> ``setAutoFlowSslEnabled``
       - ``set_auto_flow_ssl_disabled`` -> ``setAutoFlowSslDisabled``
       - ``set_h2o_node_log_level`` -> ``setH2ONodeLogLevel``
       - ``set_h2o_node_log_dir`` -> ``setH2ONodeLogDir``
       - ``set_cloud_timeout`` -> ``setCloudTimeout``
       - ``set_node_network_mask`` -> ``setNodeNetworkMask``
       - ``set_stacktrace_collector_interval`` -> ``setStacktraceCollectorInterval``
       - ``set_context_path`` -> ``setContextPath``
       - ``set_flow_scala_cell_async_enabled`` -> ``setFlowScalaCellAsyncEnabled``
       - ``set_flow_scala_cell_async_disabled`` -> ``setFlowScalaCellAsyncDisabled``
       - ``set_max_parallel_scala_cell_jobs`` -> ``setMaxParallelScalaCellJobs``
       - ``set_internal_port_offset`` -> ``setInternalPortOffset``
       - ``set_node_base_port`` -> ``setNodeBasePort``
       - ``set_mojo_destroy_timeout`` -> ``setMojoDestroyTimeout``
       - ``set_node_extra_properties`` -> ``setNodeExtraProperties``
       - ``set_flow_extra_http_headers`` -> ``setFlowExtraHttpHeaders``
       - ``set_internal_secure_connections_enabled`` -> ``setInternalSecureConnectionsEnabled``
       - ``set_internal_secure_connections_disabled`` -> ``setInternalSecureConnectionsDisabled``
       - ``set_flow_dir`` -> ``setFlowDir``
       - ``set_client_ip`` -> ``setClientIp``
       - ``set_client_iced_dir`` -> ``setClientIcedDir``
       - ``set_h2o_client_log_level`` -> ``setH2OClientLogLevel``
       - ``set_h2o_client_log_dir`` -> ``setH2OClientLogDir``
       - ``set_client_port_base`` -> ``setClientPortBase``
       - ``set_client_web_port`` -> ``setClientWebPort``
       - ``set_client_verbose_enabled`` -> ``setClientVerboseEnabled``
       - ``set_client_verbose_disabled`` -> ``setClientVerboseDisabled``
       - ``set_client_network_mask`` -> ``setClientNetworkMask``
       - ``set_ignore_spark_public_dns_enabled`` -> ``setIgnoreSparkPublicDNSEnabled``
       - ``set_ignore_spark_public_dns_disabled`` -> ``setIgnoreSparkPublicDNSDisabled``
       - ``set_client_web_enabled`` -> ``setClientWebEnabled``
       - ``set_client_web_disabled`` -> ``setClientWebDisabled``
       - ``set_client_flow_baseurl_override`` -> ``setClientFlowBaseurlOverride``
       - ``set_client_check_retry_timeout`` -> ``setClientCheckRetryTimeout``
       - ``set_client_extra_properties`` -> ``setClientExtraProperties``

- In ``H2OAutoML`` Python and Scala API, the member ``leaderboard()``/``leaderboard`` is replaced by the method ``getLeaderboard()``.

From 3.28.0 to 3.28.1
---------------------

- On ``H2OConf`` Python API, the methods ``external_write_confirmation_timeout`` and ``set_external_write_confirmation_timeout``
  are removed without replacement. On ``H2OConf`` Scala API, the methods ``externalWriteConfirmationTimeout`` and
  ``setExternalWriteConfirmationTimeout`` are removed without replacement. Also the option
  ``spark.ext.h2o.external.write.confirmation.timeout`` does not have any effect anymore.


- The location of Sparkling Water assembly JAR has changed inside the Sparkling Water distribution archive which you
  can download from our `download page <https://www.h2o.ai/download/#sparkling-water>`_.
  It has been moved from ``assembly/build/libs`` to just ``jars``.

- ``H2OSVM`` has been removed from the Scala API. We have removed this API as it was just wrapping Spark SVM and complicated
  the future development. If you still need
  to use ``SVM``, please use `Spark SVM <https://spark.apache.org/docs/latest/mllib-linear-methods.html#linear-support-vector-machines-svms>`__ directly.
  All the parameters remain the same. We are planning to expose proper
  H2O's SVM implementation in Sparkling Water in the following major releases.

- In case of binomial predictions on H2O MOJOs, the fields ``p0`` and ``p1`` in the detailed prediction column
  are replaced by a single field ``probabilities`` which is a map from label to predicted probability.
  The same is done for the fields ``p0_calibrated`` and ``p1_calibrated``. These fields are replaced
  by a single field ``calibratedProbabilities`` which is a map from label to predicted calibrated probability.

- In case of multinomial predictions on H2O MOJOs, the type of field ``probabilities`` in the detailed
  prediction column is changed from array of probabilities to a map from label to predicted probability.

- In case of ordinal predictions on H2O MOJOs, the type of field ``probabilities`` in the detailed
  prediction column is changed from array of probabilities to a map from label to predicted probability.

- On ``H2OConf`` in all clients, the methods ``externalCommunicationBlockSizeAsBytes``,
  ``externalCommunicationBlockSize`` and``setExternalCommunicationBlockSize`` have been removed as they are no longer
  needed.

- Method ``Security.enableSSL`` in Scala API has been removed. Please use
  ``setInternalSecureConnectionsEnabled`` on H2OConf to secure your cluster. This setter is
  available on Scala, Python and R clients.

From 3.26 To 3.28.0
-------------------

Passing Authentication in Scala
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The users of Scala who set up any form of authentication on the backend side are now required to specify credentials on the
``H2OConf`` object via ``setUserName`` and ``setPassword``. It is also possible to specify these directly
as Spark options ``spark.ext.h2o.user.name`` and ``spark.ext.h2o.password``. Note: Actually only users of external
backend need to specify these options at this moment as the external backend is using communication via REST api
but all our documentation is using these options already as the internal backend will start using the REST api
soon as well.

String instead of enums in Sparkling Water Algo API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- In scala, setters of the pipeline wrappers for H2O algorithms now accepts strings in places where they accepted
  enum values before. Before, we called, for example:

.. code-block:: scala

    import hex.genmodel.utils.DistributionFamily
    val gbm = H2OGBM()
    gbm.setDistribution(DistributionFamily.multinomial)


Now, the correct code is:

.. code-block:: scala

    val gbm = H2OGBM()
    gbm.setDistribution("multinomial")

which makes the Python and Scala APIs consistent. Both upper case and lower case values are valid and if a wrong
input is entered, warning is printed out with correct possible values.

Switch to Java 1.8 on Spark 2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water for Spark 2.1 now requires Java 1.8 and higher.

DRF exposed into Sparkling Water Algorithm API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

DRF is now exposed in the Sparkling Water. Please see our documentation to learn how to use it :ref:`drf`.

Also we can run our Grid Search API on DRF.

Change Default Name of Prediction Column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default name of the prediction column has been changed from ``prediction_output`` to ``prediction``.

Single value in prediction column
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The prediction column contains directly the predicted value. For example, before this change, the prediction column contained
another struct field called ``value`` (in case of regression issue), which contained the value. From now on, the predicted value
is always stored directly in the prediction column. In case of regression issue, the predicted numeric value
and in case of classification, the predicted label. If you are interested in more details created during the prediction,
please make sure to set ``withDetailedPredictionCol`` to ``true`` via the setters on both PySparkling and Sparkling Water.
When enabled, additional column named ``detailed_prediction`` is created which contains additional prediction details, such as
probabilities, contributions and so on.

In manual mode of external backend always require a specification of cluster location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In previous versions, H2O client was able to discover nodes using the multicast search.
That is now removed and IP:Port of any node of external cluster to which we need
to connect is required. This also means that in the users of multicast cloud up in case of external H2O backend in
manual standalone (no Hadoop) mode now need to pass the flatfile argument external H2O.
For more information, please see :ref:`external-backend-manual-standalone`.



Removal of Deprecated Methods and Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``getColsampleBytree`` and ``setColsampleBytree`` methods are removed from the XGBoost API. Please use
  the new ``getColSampleByTree`` and ``setColSampleByTree``.

- Removal of deprecated option ``spark.ext.h2o.external.cluster.num.h2o.nodes`` and corresponding setters.
  Please use ``spark.ext.h2o.external.cluster.size`` or the corresponding setter ``setClusterSize``.

- Removal of deprecated algorithm classes in package ``org.apache.spark.h2o.ml.algos``. Please
  use the classes from the package ``ai.h2o.sparkling.ml.algos``. Their API remains the same as before. This is the
  beginning of moving Sparkling Water classes to our distinct package ``ai.h2o.sparkling``

- Removal of deprecated option ``spark.ext.h2o.external.read.confirmation.timeout`` and related setters.
  This option is removed without a replacement as it is no longer needed.

- Removal of deprecated parameter ``SelectBestModelDecreasing`` on the Grid Search API. Related getters and setters
  have been also removed. This method is removed without replacement as we now internally sort
  the models with the ordering meaningful to the specified sort metric.

- TargetEncoder transformer now accepts the ``outputCols`` parameter which can be used to override the default output
  column names.

- On PySparkling ``H2OGLM`` API, we removed deprecated parameter ``alpha`` in favor of ``alphaValue`` and ``lambda_`` in favor of
  ``lambdaValue``. On Both PySparkling and Sparkling Water ``H2OGLM`` API, we removed methods ``getAlpha`` in favor of
  ``getAlphaValue``, ``getLambda`` in favor of ``getLambdaValue``, ``setAlpha`` in favor of ``setAlphaValue`` and
  ``setLambda`` in favor of ``setLambdaValue``. These changes ensure the consistency across Python and Scala APIs.

- In Sparkling Water ``H2OConf`` API, we removed method ``h2oDriverIf`` in favor of
  ``externalH2ODriverIf`` and  ``setH2ODriverIf`` in favor of ``setExternalH2ODriverIf``. In
  PySparkling ``H2OConf`` API, we removed method ``h2o_driver_if`` in favor of
  ``externalH2ODriverIf`` and  ``set_h2o_driver_if`` in favor of ``setExternalH2ODriverIf``.

- On PySparkling ``H2OConf`` API, the method ``user_name`` has been removed in favor of the ``userName`` method
  and method ``set_user_name`` had been removed in favor of the ``setUserName`` method.

- The configurations ``spark.ext.h2o.external.kill.on.unhealthy.interval``, ``spark.ext.h2o.external.health.check.interval``
  and ``spark.ext.h2o.ui.update.interval`` have been removed and were replaced by a single option ``spark.ext.h2o.backend.heartbeat.interval``.
  On ``H2OConf`` Scala API, the methods ``backendHeartbeatInterval`` and ``setBackendHeartbeatInterval`` were added and
  the following methods were removed: ``uiUpdateInterval``, ``setUiUpdateInterval``, ``killOnUnhealthyClusterInterval``,
  ``setKillOnUnhealthyClusterInterval``, ``healthCheckInterval`` and ``setHealthCheckInterval``. On ``H2OConf`` Python
  API, the methods ``backendHeartbeatInterval`` and ``setBackendHeartbeatInterval`` were added and
  the following methods were removed: ``ui_update_interval``, ``set_ui_update_interval``, ``kill_on_unhealthy_cluster_interval``,
  ``set_kill_on_unhealthy_cluster_interval``, ``get_health_check_interval`` and ``set_health_check_interval``. The added methods are used
  to configure single interval which was previously specified by these 3 different methods.

- The configuration ``spark.ext.h2o.cluster.client.connect.timeout`` is removed without replacement as it
  is no longer needed. on ``H2OConf`` Scala API, the methods ``clientConnectionTimeout`` and ``setClientConnectionTimeout``
  were removed and on ``H2OConf`` Python API, the methods ``set_client_connection_timeout`` and ``set_client_connection_timeout``
  were removed.

Change of Versioning Scheme
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Version of Sparkling Water is changed to the following pattern: ``H2OVersion-SWPatchVersion-SparkVersion``, where:
``H2OVersion`` is full H2O Version which is integrated to Sparkling Water. ``SWPatchVersion`` is used to specify
a patch version and ``SparkVersion`` is a Spark version. This change of scheme allows us to do releases of Sparkling Water
without the need of releasing H2O if there is only change on the Sparkling Water side. In that case, we just increment the
``SWPatchVersion``. The new version therefore looks, for example, like ``3.26.0.9-2-2.4``. This version tells us this
Sparkling Water is integrating H2O ``3.26.0.9``, it is the second release with ``3.26.0.9`` version and is for Spark ``2.4``.

Renamed Property for Passing Extra HTTP Headers for Flow UI
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The configuration property ``spark.ext.h2o.client.flow.extra.http.headers`` was renamed to
to ``spark.ext.h2o.flow.extra.http.headers`` since Flow UI can also run on H2O nodes and the value of the property is
also propagated to H2O nodes since the major version ``3.28.0.1-1``.

External Backend now keeps H2O Flow accessible on worker nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The option ``spark.ext.h2o.node.enable.web`` does not have any effect anymore for automatic mode of external
backend as we required H2O Flow to be accessible on the worker nodes. The associated getters and setters do also
not have any effect in this case.

It is also required that the users of manual mode of external backend
keep REST api available on all worker nodes. In particular, the H2O option ``-disable_web`` can't be specified
when starting H2O.

Default Values of Some AutoML Parameters Have Changed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default values of the following AutoML parameters have changed across all APIs.

+------------------------------------+------------+---------------------+
| Parameter Name                     | Old Value  | New Value           |
+====================================+============+=====================+
| ``maxRuntimeSecs``                 | ``3600.0`` | ``0.0`` (unlimited) |
+------------------------------------+------------+---------------------+
| ``keepCrossValidationPredictions`` | ``true``   | ``false``           |
+------------------------------------+------------+---------------------+
| ``keepCrossValidationModels``      | ``true``   | ``false``           |
+------------------------------------+------------+---------------------+

From any previous version to 3.26.11
------------------------------------

- Users of Sparkling Water external cluster in manual mode on Hadoop need to update the command the external cluster is launched with.
  A new parameter ``-sw_ext_backend`` needs to be added to the h2odriver invocation.

