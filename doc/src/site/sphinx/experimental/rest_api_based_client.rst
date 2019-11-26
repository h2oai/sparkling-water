Rest-API-based Client
---------------------

Supporting dynamic allocation in Sparkling Water is a long-term issue and is implemented in
several stages. The first stage is replacing H2O node running inside of the Spark executor
by lightweight REST API client.

The first iteration of replacing the H2O Node by REST API client is currently tested only
under PySparkling in External Backend Mode. At this stage we do not support using PySparkling
Algo API in this new approach. After the PySparkling Algo API is migrated to the new approach
as well, we switch to the REST-API-based client approach by default for PySparkling in external
backend.

Starting Sparkling Water with the REST API Client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable the REST-API-based client, please start PySparkling with the spark option ``spark.ext.h2o.rest.api.based.client=true``.
This ensures that there won't be any H2O client running on the Spark driver side in both manual and automatic
start of external H2O backend.

Changes on the API
~~~~~~~~~~~~~~~~~~
The API remains unaffected for the users of automatic mode of external backend.

For the users of the manual backend we have simplified the configuration and there is no need to specify a cluster size anymore in advance.
Sparkling Water automatically discovers the cluster size. In particular ``spark.ext.h2o.external.cluster.size`` is not mandatory
anymore for the users.

The rest of the API also remains unaffected, except that the PySparkling Algo API is not currently supported in the
first roll-out, but will follow up as soon as possible.

Extended H2O Jars
~~~~~~~~~~~~~~~~~
We removed the requirement of using extended H2O jars. So in both manual and automatic mode of the external backend you can
download the corresponding H2O jars for the external backend via the script ``./bin/get-h2o-driver.sh``. This script
just downloads the official H2O driver jars & they are not modified by Sparkling Water. In order to pass the Jar to
Sparkling Water in automatic mode, please use the ``H2O_DRIVER_JAR`` environmental property instead
of the ``H2O_EXTENDED_JAR``.

This should reduce the complexity of deployment and brings more transparency as well.


Benefits of the REST-API-based client
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The new approach is the base for supporting dynamic allocation, but already has several benefits:

- Extended H2O Jars should no longer be used, but original H2O driver jars.
- No need to set client network or client ip address in case the driver and the H2O cluster
  are running in different networks. We avoid connection issues related to these configurations and
  simplify the deployment
- Spark driver can now reconnect to a new or different H2O cluster. This can be done by running:

  .. code:: python

    H2OContext.getOrCreate(spark, H2OConf(spark).set_h2o_cluster(new_ip, new_port))

- H2O clusters can now be safely stopped without affecting the Spark driver

Note: the data itself are still transferred via internal protocol. This is aimed to be moved to rest api in the next
iteration.