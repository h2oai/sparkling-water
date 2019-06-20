Running RSparkling on Databricks Azure Cluster
----------------------------------------------

Sparkling Water, PySparkling and RSparkling can be used on top of Databricks Azure Cluster. This tutorial is
the **RSparkling**.

For Scala Sparkling Water, please visit `Sparkling Water on Databricks Azure Cluster <sw_azure_dbc.rst>`__ and
for PySparkling, please visit `PySparkling on Databricks Azure Cluster <pysparkling_azure_dbc.rst>`__.

To start Sparkling Water ``H2OContext`` on Databricks Azure, the steps are:

1.  Login into Microsoft Azure Portal

2.  Create Databricks Azure Environment

    In order to connect to Databricks from Azure, please make sure you have created user inside Azure Active Directory and using that user for the Databricks Login.

3.  Create the cluster

    - For Sparkling Water SUBST_SW_VERSION select Spark SUBST_SPARK_VERSION

    It is advised to always use the latest Sparkling Water and Spark version for the given Spark major version.

    .. figure:: ../images/databricks_cluster_creation.png
        :alt: Configured cluster ready to be started

4.  Create R notebook and attach it to the created cluster. To start ``H2OContext``, the init part of the notebook should be:

    .. code:: R

        # Install R packages
        install.packages("sparklyr")
        install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/rel-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_MINOR_VERSION/R")

        # Install R package for H2O SUBST_H2O_VERSION (SUBST_H2O_RELEASE_NAME)
        install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

        # Connect to Spark on Databricks
        library(rsparkling)
        library(sparklyr)
        sc <- spark_connect(method = "databricks")

        # Start H2O context
        h2o_context(sc)

6.  And voila, we should have ``H2OContext`` running

    .. figure:: ../images/databricks_rsparkling_h2o_context_running.png
        :alt: Running H2O Context

    Please note that accessing H2O Flow is not currently supported on Azure Databricks.
