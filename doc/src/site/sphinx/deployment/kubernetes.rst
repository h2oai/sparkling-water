Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside of the Kubernetes cluster. Please note that we currently
support only internal backend of sparkling water with cluster deployment mode, which means
that both executors and driver are running inside Kubernetes. Sparkling Water supports
Kubernetes since Spark version 2.3.

Prerequisites:
 - Sparkling Water Distribution SUBST_SW_VERSION
 - Kubernetes Cluster
 - Apache Spark SUBST_SPARK_VERSION

To start Sparkling Water on Kubernetes, the steps are:

0. Please make yourself familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

1. Ensure you have ``SPARK_HOME`` set up to home of your Spark distribution.

2. Run ``./bin/build-kubernetes-images.sh`` script inside of the Sparkling Water distribution.
   This step builds docker images for all Sparkling Water, PySparkling and RSparkling which are
   later used as the base image for the Kubernetes pods. Please also make sure that your Docker
   environment is the one managed by Kubernetes as Kubernetes needs to see the created images.
   You can also extend these images by putting inside our application dependencies.

3. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

4. Start Sparkling Water with 3 worker nodes:


.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master k8s://IP:PORT \
            --deploy-mode cluster \
            --name CustomApplication \
            --class custom.app.class \
            --conf spark.kubernetes.container.image=sparkling-water-scala:${SUBST_SW_VERSION} \
            --conf spark.executor.instances=3 \
            local:///opt/app.jar


    .. tab-container:: Python
        :title: Python

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master k8s://IP:PORT \
            --deploy-mode cluster \
            --name CustomApplication \
            --conf spark.kubernetes.container.image=sparkling-water-scala:${SUBST_SW_VERSION} \
            --conf spark.executor.instances=3 \
            local:///opt/app.py


The ``IP:PORT`` represents the Kubernetes master obtained in step 3. It is important to mention
that the application resource (the last argument) needs to be available in the docker image.

After this step, your job is submitted into Kubernetes cluster. You can see the logs by running
``kubectl logs pod_id``, where you can get the pod id of the desired executor or driver by
running ``kubectl get pods``.
