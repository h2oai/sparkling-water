Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside the Kubernetes cluster. Both cluster and client deployment modes
are supported. Sparkling Water supports Kubernetes since Spark version 2.4.

Before you start, please make check the following:

0. Please make yourself familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

1. Ensure that you have working Kubernetes Cluster and ``kubectl`` installed

1. Ensure you have ``SPARK_HOME`` set up to home of your Spark distribution of version SUBST_SPARK_VERSION

2. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

Internal backend
~~~~~~~~~~~~~~~~

In internal backend of Sparkling Water, we need to pas the option ``spark.scheduler.minRegisteredResourcesRatio=1``
to your Spark job invocation. This ensures that Spark waits for all resources and therefore Sparkling Water will
start H2O on all requested executors.

Dynamic allocation must be disabled in Spark.

Start Interactive Shell (client mode):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Download Sparkling distribution from `H2O download page <https://s3.amazonaws.com/h2o-release/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/index.html>`_
and extract it. Let's assume the path to extracted distribution is in env variable ``SW_HOME``.

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
                --jars $SW_HOME/jars/sparkling-water-assembly_2.11-SUBST_SW_VERSION-all


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
                --py-files $SW_HOME/py/build/dist/h2o_pysparkling_SUBST_SPARK_MAJOR_VERSION-SUBST_SW_VERSION.zip


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
