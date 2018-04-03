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

3.  Upload Sparkling Water assembly JAR as a library
    Even though we are using RSparkling, we need to explicitly upload the Sparkling Water assembly Jar as it is a required dependency. In order to create the library in Databricks, go to **Libraries**, select **Upload Java/Scala JAR** and upload the downloaded assembly jar. If you download the official distribution, the assembly jar is located in ``assembly/build/libs`` directory. The assembly Jar can be downloaded from our official `Download page <https://www.h2o.ai/download/>`__.

    .. figure:: ../images/databricks_upload_jar.png
        :alt: Uploading Sparkling Water assembly JAR

    You can configure each cluster manually and select which libraries should be attached or you can configure the library to be attached to all future clusters. It is advised to restart the cluster in case you attached the library to already running cluster to ensure the clean environment.

4.  Create the cluster

    - Make sure the assembly JAR is attached to the cluster

    - For Sparkling Water SUBST_SW_VERSION select Spark SUBST_SPARK_VERSION

    It is advised to always use the latest Sparkling Water and Spark version for the given Spark major version.

    .. figure:: ../images/databricks_cluster_creation.png
        :alt: Configured cluster ready to be started

5.  Create Scala notebook and attach it to the created cluster. To start ``H2OContext``, the init part of the notebook should be:

    .. code:: R

        # Install R packages
        install.packages("sparklyr")
        install.packages("rsparkling")

        # Now we download, install, and initialize the H2O package for R.
        # Make sure to install H2O with the same version as its bundled inside Sparkling Water. The version table can be seen
        # at https://github.com/h2oai/rsparkling#install-h2o. In This case, we are using Sparkling Water SUBST_SW_VERSION which is using
        # H2O SUBST_H2O_VERSION (SUBST_H2O_RELEASE_NAME)
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
