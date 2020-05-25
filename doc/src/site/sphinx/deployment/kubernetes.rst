Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside of the Kubernetes cluster. Please note that we currently
support only internal backend of sparkling water with cluster deployment mode, which means
that both executors and driver are running inside Kubernetes. Sparkling Water supports
Kubernetes since Spark version 2.4.

Prerequisites:
 - Sparkling Water Distribution SUBST_SW_VERSION
 - Kubernetes Cluster
 - Apache Spark SUBST_SPARK_VERSION

To start Sparkling Water on Kubernetes, the steps are:

0. Please make yourself familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

1. Ensure you have ``SPARK_HOME`` set up to home of your Spark distribution.

2. Create the Sparkling Water base image for Kubernetes:

    Run ``./bin/build-kubernetes-images.sh`` script inside of the Sparkling Water distribution, which can be downloaded
    from `H2O Download page <https://www.h2o.ai/download/>`__.
    The script takes one argument which can be either ``scala``, ``python`` or ``r`` and creates a docker image
    for that specific Sparkling Water client.

    Note: Make sure that your Docker environment is the one managed by Kubernetes as Kubernetes needs to see the created images.

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

            .. code:: bash

                ./bin/build-kubernetes-images.sh scala

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                ./bin/build-kubernetes-images.sh python


3. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

4. Create a custom Dockerfile with your application resources inside and build the image:

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

            .. code:: bash

                cat <<EOT > Dockerfile-Scala-CustomApp
                FROM sparkling-water-scala:SUBST_SW_VERSION
                COPY ./app.jar "/opt/app.jar"
                EOT

                docker build -t "sparkling-water-scala-custom-app:SUBST_SW_VERSION" -f Dockerfile-Scala-CustomApp .

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                cat <<EOT > Dockerfile-Python-CustomApp
                FROM sparkling-water-python:SUBST_SW_VERSION
                COPY ./app.py "/opt/app.py"
                EOT

                docker build -t "sparkling-water-python-custom-app:SUBST_SW_VERSION" -f  Dockerfile-Python-CustomApp .


5. Start Sparkling Water with 3 worker nodes:

    .. content-tabs::

        .. tab-container:: Scala
            :title: Scala

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://IP:PORT \
                --deploy-mode cluster \
                --name CustomApplication \
                --class custom.app.class \
                --conf spark.kubernetes.container.image=sparkling-water-scala-custom-app:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \
                local:///opt/app.jar

        .. tab-container:: Python
            :title: Python

            .. code:: bash

                $SPARK_HOME/bin/spark-submit \
                --master k8s://IP:PORT \
                --deploy-mode cluster \
                --name CustomApplication \
                --conf spark.kubernetes.container.image=sparkling-water-python-custom-app:SUBST_SW_VERSION \
                --conf spark.executor.instances=3 \
                local:///opt/app.py


The ``IP:PORT`` represents the Kubernetes master obtained in step 3. It is important to mention
that the application resource (the last argument) needs to be available in the docker image.

After this step, your job is submitted into Kubernetes cluster. You can see the logs by running
``kubectl logs pod_id``, where you can get the pod id of the desired executor or driver by
running ``kubectl get pods``.
