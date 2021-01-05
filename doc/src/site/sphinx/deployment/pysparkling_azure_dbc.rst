.. _pysparkling_azure:

Running PySparkling on Databricks Azure Cluster
-----------------------------------------------

Sparkling Water, PySparkling and RSparkling can be used on top of Databricks Azure Cluster. This tutorial is
the **PySparkling**.

For Scala Sparkling Water, please visit :ref:`sw_azure` and
for RSparkling, please visit :ref:`rsparkling_azure`.

To start Sparkling Water ``H2OContext`` on Databricks Azure, the steps are:

1.  Login into Microsoft Azure Portal

2.  Create Databricks Azure Environment

    In order to connect to Databricks from Azure, please make sure you have created the user inside Azure Active Directory and using that user for the Databricks Login.

3.  Add PySparkling dependency

    In order to create the Python library in Databricks, go to **Libraries**, select **PiPy** as the library source and type in: ``h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION`` for the latest PySparkling for Spark SUBST_SPARK_MAJOR_VERSION.

    If you don't want to train new models and just score with existing H2O-3, SW, Driverless AI MOJO models, you can use a smaller package called ``h2o_pysparkling_scoring_SUBST_SPARK_MAJOR_VERSION`` instead.

    The advantage of adding the library from PiPy, instead of uploading manually, is that the latest version is always selected and also, the dependencies are automatically resolved.

    .. figure:: ../images/databricks_pysparkling_pipy.png
        :alt: Uploading PySparkling Library

    You can configure each cluster manually and select which libraries should be attached or you can configure the library to be attached to all future clusters. It is advised to restart the cluster in case you attached the library to the already running cluster to ensure the clean environment.

4.  Create the cluster

    - Make sure the library is attached to the cluster

    - Select Spark SUBST_SPARK_VERSION

    .. figure:: ../images/databricks_cluster_creation.png
        :alt: Example of configured cluster ready to be started

5.  Create a Python notebook and attach it to the created cluster. To start ``H2OContext``, the init part of the notebook should be:

    .. code:: python

        from pysparkling import *
        hc = H2OContext.getOrCreate()

6.  And voila, we should have ``H2OContext`` running

    .. figure:: ../images/databricks_sw_h2o_context_running.png
        :alt: Running H2O Context

7. Flow is accessible via the URL printed out after H2OContext is started. Internally we use
   open port 9009. If you have an environment where a different port is open on your Azure Databricks
   cluster, you can configure it via ``spark.ext.h2o.client.web.port`` or corresponding setter
   on ``H2OConf``.
