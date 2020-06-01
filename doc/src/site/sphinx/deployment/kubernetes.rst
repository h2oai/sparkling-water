Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside the Kubernetes cluster. Only cluster deployment mode is supported at this
moment. Sparkling Water supports Kubernetes since Spark version 2.4.

Before you start, please make check the following:

1. Please make yourself familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

2. Ensure that you have working Kubernetes Cluster and ``kubectl`` installed

3. Ensure you have ``SPARK_HOME`` set up to home of your Spark distribution of version SUBST_SPARK_VERSION

4. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

5. Have internet connection so kubernetes can download Sparkling Water docker images

The examples bellow are using the default Kubernetes namespace which we enable for Spark as:

.. code:: bash

    kubectl create clusterrolebinding default --clusterrole=edit --serviceaccount=default:default --namespace=default

You can also use different namespace setup for Spark. In that case please don't forget to pass
``--conf spark.kubernetes.authenticate.driver.serviceAccountName=serviceName`` to your Spark commands.

Internal backend
~~~~~~~~~~~~~~~~

In internal backend of Sparkling Water, we need to pas the option ``spark.scheduler.minRegisteredResourcesRatio=1``
to your Spark job invocation. This ensures that Spark waits for all resources and therefore Sparkling Water will
start H2O on all requested executors.

Dynamic allocation must be disabled in Spark.

RSparkling in Internal Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In case of RSparkling, SparklyR automatically sets the Spark deployment mode and it is not possible to specify it.
It is also possible to run only interactive sessions.

To start ``H2OContext`` in interactive shell:

.. code:: r

    library(sparklyr)
    library(rsparkling)
    config = spark_config_kubernetes("k8s://KUBERNETES_ENDPOINT",
                     image = "h2oai/sparkling-water-r:SUBST_SW_VERSION",
                     account = "default",
                     executors = 3,
                     version = "SUBST_SPARK_VERSION",
                     ports = c(8880, 8881, 4040, 54321))
    config["spark.home"] <- Sys.getenv("SPARK_HOME")
    sc <- spark_connect(config = config, spark_home = Sys.getenv("SPARK_HOME"))
    hc <- H2OContext.getOrCreate()
    spark_disconnect(sc)

PySparkling in Internal Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Currently, only cluster deployment mode of Kubernetes is supported.

To submit Python job, run:

.. code:: bash

    $SPARK_HOME/bin/spark-submit \
    --master k8s://KUBERNETES_ENDPOINT \
    --deploy-mode cluster \
    --name CustomApplication \
    --conf spark.scheduler.minRegisteredResourcesRatio=1
    --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
    --conf spark.executor.instances=3 \
    local:///opt/sparkling-water/tests/initTest.py

Sparkling Water in Internal Backend
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Currently, only cluster deployment mode of Kubernetes is supported.

To submit Scala job:

.. code:: bash

    $SPARK_HOME/bin/spark-submit \
    --master k8s://KUBERNETES_ENDPOINT \
    --deploy-mode cluster \
    --name CustomApplication \
    --class ai.h2o.sparkling.InitTest
    --conf spark.scheduler.minRegisteredResourcesRatio=1
    --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
    --conf spark.executor.instances=3 \
    local:///opt/sparkling-water/tests/initTest.jar
