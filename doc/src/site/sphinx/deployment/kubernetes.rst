Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside the Kubernetes cluster. Both cluster and client deployment modes
are supported. Sparkling Water supports Kubernetes since Spark version 2.4.

Prerequisites:
 - Sparkling Water Distribution SUBST_SW_VERSION
 - Kubernetes Cluster and ``kubectl`` installed
 - Apache Spark SUBST_SPARK_VERSION

Before you start, please make check the following:

0. Please make yourself familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

1. Ensure you have ``SPARK_HOME`` set up to home of your Spark distribution.

2. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

Internal backend
~~~~~~~~~~~~~~~~

In internal backend of Sparkling Water, we recommend passing the option ``spark.scheduler.minRegisteredResourcesRatio=1``
to your invocation. This ensures that Spark waits for all resources and therefore Sparkling Water will start H2O on all
requested executors.

Dynamic allocation must be disabled in Spark.

Start Interactive Shell (client mode):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To start Sparkling Water with 3 worker nodes:

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://KUBERNETES_ENDPOINT \
                --deploy-mode client \
                --name CustomApplication \
                --conf spark.scheduler.minRegisteredResourcesRatio=1
                --conf spark.kubernetes.container.image=sparkling-water-scala-custom-app:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://KUBERNETES_ENDPOINT \
                --deploy-mode client \
                --name CustomApplication \
                --conf spark.scheduler.minRegisteredResourcesRatio=1
                --conf spark.kubernetes.container.image=sparkling-water-python-custom-app:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \

After this step, your job is submitted into Kubernetes cluster. You can see the logs by running
``kubectl logs pod_id``. You can get the pod id of the desired executor or driver by
running ``kubectl get pods``.

Submit Batch Jobs (cluster mode):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To submit Sparkling Water Job with 3 worker nodes:

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://KUBERNETES_ENDPOINT \
                --deploy-mode client \
                --name CustomApplication \
                --class ai.h2o.sparkling.InitTest
                --conf spark.scheduler.minRegisteredResourcesRatio=1
                --conf spark.kubernetes.container.image=sparkling-water-scala:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \
                local:///opt/sparkling-water/tests/initTest.jar

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://KUBERNETES_ENDPOINT \
                --deploy-mode client \
                --name CustomApplication \
                --conf spark.scheduler.minRegisteredResourcesRatio=1
                --conf spark.kubernetes.container.image=sparkling-water-python:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \
                local:///opt/sparkling-water/tests/initTest.py

After this step, your job is submitted into Kubernetes cluster. You can see the logs by running
``kubectl logs pod_id``. You can get the pod id of the desired executor or driver by
running ``kubectl get pods``.
