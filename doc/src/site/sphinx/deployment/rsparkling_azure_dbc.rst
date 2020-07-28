.. _rsparkling_azure:

Running RSparkling on Databricks Azure Cluster
----------------------------------------------

Sparkling Water, PySparkling and RSparkling can be used on top of Databricks Azure Cluster. This tutorial is
the **RSparkling**.

For Scala Sparkling Water, please visit :ref:`sw_azure` and
for PySparkling, please visit :ref:`pysparkling_azure`.

To start Sparkling Water ``H2OContext`` on Databricks Azure, the steps are:

1.  Login into Microsoft Azure Portal

2.  Create Databricks Azure Environment

    In order to connect to Databricks from Azure, please make sure you have created the user inside Azure Active Directory and using that user for the Databricks Login.

3.  Create the cluster

    - For Sparkling Water SUBST_SW_VERSION select Spark SUBST_SPARK_VERSION

    It is advised to always use the latest Sparkling Water and Spark version for the given Spark major version.

    .. figure:: ../images/databricks_cluster_creation.png
        :alt: Configured cluster ready to be started

4.  Create a R notebook and attach it to the created cluster. To start ``H2OContext``, the init part of the notebook should be:

    .. code:: R

        # Install Sparklyr
        install.packages("sparklyr")

        # Install H2O SUBST_H2O_VERSION (SUBST_H2O_RELEASE_NAME)
        install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

        # Install RSparkling SUBST_SW_VERSION
        install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")

        # Connect to Spark on Databricks
        library(rsparkling)
        library(sparklyr)
        sc <- spark_connect(method = "databricks")

        # Start H2O context
        h2o_context(sc)

6.  And voila, we should have ``H2OContext`` running

    .. figure:: ../images/databricks_rsparkling_h2o_context_running.png
        :alt: Running H2O Context

7. Flow is accessible via the URL printed out after H2OContext is started. Internally we use
   open port 9009. If you have an environment where a different port is open on your Azure Databricks
   cluster, you can configure it via ``spark.ext.h2o.client.web.port``.
