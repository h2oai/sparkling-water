Running Sparkling Water in Kubernetes
-------------------------------------

Sparkling Water can be executed inside the Kubernetes cluster. Sparkling Water supports Kubernetes since Spark version 2.4.

Before we start, please check the following:

1. Please make sure we are familiar with how to run Spark on Kubernetes at
   `Spark Kubernetes documentation <https://spark.apache.org/docs/SUBST_SPARK_VERSION/running-on-kubernetes.html>`__.

2. Ensure that we have a working Kubernetes Cluster and ``kubectl`` installed

3. Ensure we have ``SPARK_HOME`` set up to a home directory of our Spark distribution of version SUBST_SPARK_VERSION

4. Run ``kubectl cluster-info`` to obtain Kubernetes master URL.

5. Have internet connection so Kubernetes can download Sparkling Water docker images

6. If we have some non-default network policies applied to the namespace where Sparkling Water is supposed to run,
   make sure that the following ports are exposed: all Spark ports and ports 54321 and 54322 as these are
   also necessary by H2O to be able to communicate.

The examples below are using the default Kubernetes namespace which we enable for Spark as:

.. code:: bash

    kubectl create clusterrolebinding default --clusterrole=edit --serviceaccount=default:default --namespace=default

We can also use different namespace setup for Spark. In that case please don't forget to pass
``--conf spark.kubernetes.authenticate.driver.serviceAccountName=serviceName`` to our Spark commands.

Internal Backend
~~~~~~~~~~~~~~~~

In the internal backend of Sparkling Water, we need to pass the option ``spark.scheduler.minRegisteredResourcesRatio=1``
to our Spark job invocation. This ensures that Spark waits for all resources and therefore Sparkling Water will
start H2O on all requested executors.

Dynamic allocation must be disabled in Spark.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Scala job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/spark-shell \
             --master "k8s://KUBERNETES_ENDPOINT" \
             --deploy-mode client \
             --conf spark.scheduler.minRegisteredResourcesRatio=1 \
             --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
             --conf spark.executor.instances=3 \
             --conf spark.driver.host=sparkling-water-app \
             --conf spark.kubernetes.driver.pod.name=sparkling-water-app

        4. Inside the shell, run:

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

    .. tab-container:: Python
        :title: Python

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Python job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            local:///opt/sparkling-water/tests/initTest.py

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node:

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/pyspark \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app

        4. Inside the shell, run:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod as:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            local:///opt/sparkling-water/tests/initTest.py

    .. tab-container:: R
        :title: R

        First, make sure that RSparkling is installed on the node we want to run RSparkling from.
        You can install RSparkling as:

        .. code:: r

           # Download, install, and initialize the H2O package for R.
           # In this case we are using rel-SUBST_H2O_RELEASE_NAME SUBST_H2O_BUILD_NUMBER (SUBST_H2O_VERSION)
           install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

           # Download, install, and initialize the RSparkling
           install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")


        To start ``H2OContext`` in an interactive shell, run the following code in R or RStudio:

        .. code:: r

            library(sparklyr)
            library(rsparkling)
            config <- spark_config_kubernetes("k8s://KUBERNETES_ENDPOINT",
                             image = "h2oai/sparkling-water-r:SUBST_SW_VERSION",
                             account = "default",
                             executors = 3,
                             conf = list("spark.kubernetes.file.upload.path"="file:///tmp"),
                             version = "SUBST_SPARK_VERSION",
                             ports = c(8880, 8881, 4040, 54321))
            config["spark.home"] <- Sys.getenv("SPARK_HOME")
            sc <- spark_connect(config = config, spark_home = Sys.getenv("SPARK_HOME"))
            hc <- H2OContext.getOrCreate()
            spark_disconnect(sc)

        You can also submit RSparkling batch job. In that case, create a file called `batch.R` with the content
        from the code box above and run:

        .. code:: bash

            Rscript --default-packages=methods,utils batch.R

        Note: In the case of RSparkling, SparklyR automatically sets the Spark deployment mode and it is not possible to specify it.

Manual Mode of External Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water External backend can be also used in Kubernetes. First, we need to start
an external H2O backend on Kubernetes. To achieve this, please follow the steps on the
`H2O on Kubernetes Documentation <https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/welcome.html#kubernetes-integration/>`__ with
**one important exception**. The image to be used need to be `h2oai/sparkling-water-external-backend:SUBST_SW_VERSION` and not the base H2O image as mentioned in
H2O documentation as Sparkling Water enhances the H2O image with additional dependencies.

In order for Sparkling Water to be able to connect to the H2O cluster, we need to get the address of the leader node
of the H2O cluster. If we followed the H2O documentation on how to start H2O cluster on Kubernetes, the address is
``h2o-service.default.svc.cluster.local:54321`` where the first part is the H2O headless service name and the second part is the name
of the namespace.

After we created the external H2O backend, we can connect to it from Sparkling Water clients as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Scala job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/spark-shell \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root

        4. Inside the shell, run:

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

    .. tab-container:: Python
        :title: Python

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Python job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root \
            local:///opt/sparkling-water/tests/initTest.py

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node:

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/pyspark \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root

        4. Inside the shell, run:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod as:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=manual \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.cloud.representative=h2o-service.default.svc.cluster.local:54321 \
            --conf spark.ext.h2o.cloud.name=root \
            local:///opt/sparkling-water/tests/initTest.py

    .. tab-container:: R
        :title: R

        First, make sure that RSparkling is installed on the node we want to run RSparkling from.
        You can install RSparkling as:

        .. code:: r

           # Download, install, and initialize the H2O package for R.
           # In this case we are using rel-SUBST_H2O_RELEASE_NAME SUBST_H2O_BUILD_NUMBER (SUBST_H2O_VERSION)
           install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

           # Download, install, and initialize the RSparkling
           install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")

        To start ``H2OContext`` in an interactive shell, run the following code in R or RStudio:

        .. code:: r

            library(sparklyr)
            library(rsparkling)
            config <- spark_config_kubernetes("k8s://KUBERNETES_ENDPOINT",
                             image = "h2oai/sparkling-water-r:SUBST_SW_VERSION",
                             account = "default",
                             executors = 2,
                             version = "SUBST_SPARK_VERSION",
                             conf = list(
                                     "spark.ext.h2o.backend.cluster.mode"="external",
                                     "spark.ext.h2o.external.start.mode"="manual",
                                     "spark.ext.h2o.external.memory"="2G",
                                     "spark.ext.h2o.cloud.representative"="h2o-service.default.svc.cluster.local:54321",
                                     "spark.ext.h2o.cloud.name"="root",
                                     "spark.kubernetes.file.upload.path"="file:///tmp"),
                             ports = c(8880, 8881, 4040, 54321))
            config["spark.home"] <- Sys.getenv("SPARK_HOME")
            sc <- spark_connect(config = config, spark_home = Sys.getenv("SPARK_HOME"))
            hc <- H2OContext.getOrCreate()
            spark_disconnect(sc)

        You can also submit RSparkling batch job. In that case, create a file called `batch.R` with the content
        from the code box above and run:

        .. code:: bash

            Rscript --default-packages=methods,utils batch.R

        Note: In the case of RSparkling, SparklyR automatically sets the Spark deployment mode and it is not possible to specify it.

Automatic Mode of External Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the automatic mode, Sparkling Water starts external H2O on Kubernetes automatically. The requirement is that the
driver node is configured to communicate with the Kubernetes cluster. Docker image for the external H2O backend
is specified using the ``spark.ext.h2o.external.k8s.docker.image`` option.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Scala job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/spark-shell \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION

        4. Inside the shell, run:

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-scala:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION \
            --class ai.h2o.sparkling.KubernetesTest \
            local:///opt/sparkling-water/tests/kubernetesTest.jar

    .. tab-container:: Python
        :title: Python

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Python job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION \
            local:///opt/sparkling-water/tests/initTest.py

        **To start an interactive shell in a client mode:**

        1. Create Headless service so Spark executors can reach the driver node:

        .. code:: bash

            cat <<EOF | kubectl apply -f -
            apiVersion: v1
            kind: Service
            metadata:
              name: sparkling-water-app
            spec:
              clusterIP: "None"
              selector:
                spark-driver-selector: sparkling-water-app
            EOF

        2. Start pod from where we run the shell:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/pyspark \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION

        4. Inside the shell, run:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod as:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First, create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-driver-selector=sparkling-water-app --image=h2oai/sparkling-water-python:SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
            --master "k8s://KUBERNETES_ENDPOINT" \
            --deploy-mode client \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=2 \
            --conf spark.driver.host=sparkling-water-app \
            --conf spark.kubernetes.driver.pod.name=sparkling-water-app \
            --conf spark.ext.h2o.backend.cluster.mode=external \
            --conf spark.ext.h2o.external.start.mode=auto \
            --conf spark.ext.h2o.external.auto.start.backend=kubernetes \
            --conf spark.ext.h2o.external.cluster.size=2 \
            --conf spark.ext.h2o.external.memory=2G \
            --conf spark.ext.h2o.external.k8s.docker.image=h2oai/sparkling-water-external-backend:SUBST_SW_VERSION \
            local:///opt/sparkling-water/tests/initTest.py

    .. tab-container:: R
        :title: R

        First, make sure that RSparkling is installed on the node we want to run RSparkling from.
        You can install RSparkling as:

        .. code:: r

           # Download, install, and initialize the H2O package for R.
           # In this case we are using rel-SUBST_H2O_RELEASE_NAME SUBST_H2O_BUILD_NUMBER (SUBST_H2O_VERSION)
           install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

           # Download, install, and initialize the RSparkling
           install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")

        To start ``H2OContext`` in an interactive shell, run the following code in R or RStudio:

        .. code:: r

            library(sparklyr)
            library(rsparkling)
            config <- spark_config_kubernetes("k8s://KUBERNETES_ENDPOINT",
                             image = "h2oai/sparkling-water-r:SUBST_SW_VERSION",
                             account = "default",
                             executors = 2,
                             version = "SUBST_SPARK_VERSION",
                             conf = list(
                                     "spark.ext.h2o.backend.cluster.mode"="external",
                                     "spark.ext.h2o.external.start.mode"="auto",
                                     "spark.ext.h2o.external.auto.start.backend"="kubernetes",
                                     "spark.ext.h2o.external.memory"="2G",
                                     "spark.ext.h2o.external.cluster.size"="2",
                                     "spark.ext.h2o.external.k8s.docker.image"="h2oai/sparkling-water-external-backend:SUBST_SW_VERSION",
                                     "spark.kubernetes.file.upload.path"="file:///tmp"),
                             ports = c(8880, 8881, 4040, 54321))
            config["spark.home"] <- Sys.getenv("SPARK_HOME")
            sc <- spark_connect(config = config, spark_home = Sys.getenv("SPARK_HOME"))
            hc <- H2OContext.getOrCreate()
            spark_disconnect(sc)

        You can also submit RSparkling batch job. In that case, create a file called `batch.R` with the content
        from the code box above and run:

        .. code:: bash

            Rscript --default-packages=methods,utils batch.R

        Note: In the case of RSparkling, SparklyR automatically sets the Spark deployment mode and it is not possible to specify it.
