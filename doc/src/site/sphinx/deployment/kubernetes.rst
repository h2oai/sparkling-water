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

Cluster Mode of Kubernetes
^^^^^^^^^^^^^^^^^^^^^^^^^^

In case of Scala and Python, in cluster mode we can only submit batch jobs. In case of R we
can submit batch jobs or use interactive shell. This is allowed because internal SparklyR
implementation can connect to a remote Spark cluster.

To submit Sparkling Water Job with 3 worker nodes:

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

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

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://KUBERNETES_ENDPOINT \
                --deploy-mode cluster \
                --name CustomApplication \
                --conf spark.scheduler.minRegisteredResourcesRatio=1
                --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \
                local:///opt/sparkling-water/tests/initTest.py

        .. tab-container:: R
            :title: R

            To start ``H2OContext`` in interactive shell:

            .. code:: r

                library(sparklyr)
                library(rsparkling)
                config = spark_config_kubernetes("k8s://KUBERNETES_ENDPOINT",
                                 image = "h2oai/sparkling-water-r:SUBST_SW_VERSION",
                                 account = "default",
                                 executors = 3)
                hc <- H2OContext.getOrCreate()

After this step, your job is submitted into Kubernetes cluster. You can see the logs by running
``kubectl logs pod_id``. You can get the pod id of the desired executor or driver by
running ``kubectl get pods``.
