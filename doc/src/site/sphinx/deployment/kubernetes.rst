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

6. If you have non-default network policies applied to the namespace where Sparkling Water is supposed to run,
   make sure that the following ports are exposed: all Spark ports and ports 54321 and 54322 as these are
   also necessary by H2O to be able to communicate.

The examples bellow are using the default Kubernetes namespace which we enable for Spark as:

.. code:: bash

    kubectl create clusterrolebinding default --clusterrole=edit --serviceaccount=default:default --namespace=default

You can also use different namespace setup for Spark. In that case please don't forget to pass
``--conf spark.kubernetes.authenticate.driver.serviceAccountName=serviceName`` to your Spark commands.

Internal Backend
~~~~~~~~~~~~~~~~

In internal backend of Sparkling Water, we need to pas the option ``spark.scheduler.minRegisteredResourcesRatio=1``
to your Spark job invocation. This ensures that Spark waits for all resources and therefore Sparkling Water will
start H2O on all requested executors.

Dynamic allocation must be disabled in Spark.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Both cluster and client deployment modes of Kubernetes are supported.

        **To submit Scala job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master k8s://KUBERNETES_ENDPOINT \
            --deploy-mode cluster \
            --class ai.h2o.sparkling.InitTest \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            local:///opt/sparkling-water/tests/initTest.jar

        **To start a interactive shell in a client mode:**

        1. Create Headless so Spark executors can reach the driver node

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

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-app-selector=yoursparkapp --image=h2oai/sparkling-water:scala-SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/spark-shell \
             --conf spark.scheduler.minRegisteredResourcesRatio=1 \
             --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
             --master "k8s://KUBERNETES_ENDPOINT" \
             --conf spark.driver.host=sparkling-water-app \
             --deploy-mode client \
             --conf spark.executor.instances=3

        4. Inside the shell, run:

        .. code:: python

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-app-selector=yoursparkapp --image=h2oai/sparkling-water:scala-SUBST_SW_VERSION -- /bin/bash \
            /opt/spark/bin/spark-submit \
             --conf spark.scheduler.minRegisteredResourcesRatio=1 \
             --conf spark.kubernetes.container.image=h2oai/sparkling-water-scala:SUBST_SW_VERSION \
             --master "k8s://KUBERNETES_ENDPOINT" \
             --class ai.h2o.sparkling.InitTest \
             --conf spark.driver.host=sparkling-water-app \
             --deploy-mode client \
             --conf spark.executor.instances=3 \
            local:///opt/sparkling-water/tests/initTest.jar

    .. tab-container:: Python
        :title: Python

        Both cluster and client deployment mode of Kubernetes are supported.

        **To submit Python job in a cluster mode, run:**

        .. code:: bash

            $SPARK_HOME/bin/spark-submit \
            --master k8s://KUBERNETES_ENDPOINT \
            --deploy-mode cluster \
            --conf spark.scheduler.minRegisteredResourcesRatio=1 \
            --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
            --conf spark.executor.instances=3 \
            local:///opt/sparkling-water/tests/initTest.py

        **To start a interactive shell in a client mode:**

        1. Create Headless so Spark executors can reach the driver node:

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

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-app-selector=yoursparkapp --image=h2oai/sparkling-water:python-SUBST_SW_VERSION -- /bin/bash

        3. Inside the container, start the shell:

        .. code:: bash

            $SPARK_HOME/bin/pyspark \
             --conf spark.scheduler.minRegisteredResourcesRatio=1 \
             --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
             --master "k8s://KUBERNETES_ENDPOINT" \
             --conf spark.driver.host=sparkling-water-app \
             --deploy-mode client \
             --conf spark.executor.instances=3 \

        4. Inside the shell, run:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        5. To access flow, we need to enable port-forwarding from the driver pod as:

        .. code:: bash

            kubectl port-forward sparkling-water-app 54321:54321

        **To submit a batch job using client mode:**

        First create the headless service as mentioned in the step 1 above and run:

        .. code:: bash

            kubectl run -n default -i --tty sparkling-water-app --restart=Never --labels spark-app-selector=yoursparkapp --image=h2oai/sparkling-water:python-SUBST_SW_VERSION -- \
            $SPARK_HOME/bin/spark-submit \
             --conf spark.scheduler.minRegisteredResourcesRatio=1 \
             --conf spark.kubernetes.container.image=h2oai/sparkling-water-python:SUBST_SW_VERSION \
             --master "k8s://KUBERNETES_ENDPOINT" \
             --conf spark.driver.host=sparkling-water-app \
             --deploy-mode client \
             --conf spark.executor.instances=3 \
            local:///opt/sparkling-water/tests/initTest.py

    .. tab-container:: R
        :title: R

        First, make sure that RSparkling is installed on the node you want to run RSparkling from.
        You can install RSparkling as:

        .. code:: r

           # Download, install, and initialize the H2O package for R.
           # In this case we are using rel-SUBST_H2O_RELEASE_NAME SUBST_H2O_BUILD_NUMBER (SUBST_H2O_VERSION)
           install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

           # Download, install, and initialize the RSparkling
           install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")


        To start ``H2OContext`` in interactive shell, run the following code in R or RStudio:

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

        You can also submit RSparkling batch job. In that case create a file called `batch.R` with the content
        from the code box above and run:

        .. code:: r

            Rscript --default-packages=methods,utils batch.R

        Note: In case of RSparkling, SparklyR automatically sets the Spark deployment mode and it is not possible to specify it.

External Backend
~~~~~~~~~~~~~~~~

Sparkling Water External backend can be also used in Kubernetes. First, we need to start external H2O backend on
Kubernetes. This can be achieved by the following steps:

1. Create Headless Service for H2O node discovery

.. code:: bash

    cat <<EOF | kubectl apply -f -
    apiVersion: v1
    kind: Service
    metadata:
      name: h2o-service
    spec:
      type: ClusterIP
      clusterIP: None
      selector:
        app: h2o-k8s
      ports:
      - protocol: TCP
        port: 54321
    EOF

2. Deploy external H2O cluster of size 2 as:

.. code:: bash

    cat <<EOF | kubectl apply -f -
    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: h2o-stateful-set
      namespace: default
    spec:
      serviceName: h2o-service
      replicas: 2
      selector:
        matchLabels:
          app: h2o-k8s
      template:
        metadata:
          labels:
            app: h2o-k8s
        spec:
          terminationGracePeriodSeconds: 10
          containers:
            - name: h2o-k8s
              image: 'h2oai/sparkling-water-external-backend:SUBST_SW_VERSION'
              resources:
                requests:
                  memory: "2Gi"
              ports:
                - containerPort: 54321
                  protocol: TCP
              readinessProbe:
                httpGet:
                  path: /kubernetes/isLeaderNode
                  port: 8081
                initialDelaySeconds: 5
                periodSeconds: 5
                failureThreshold: 1
              env:
              - name: H2O_KUBERNETES_SERVICE_DNS
                value: h2o-service.default.svc.cluster.local
              - name: H2O_NODE_LOOKUP_TIMEOUT
                value: '180'
              - name: H2O_NODE_EXPECTED_COUNT
                value: '2'
              - name: H2O_KUBERNETES_API_PORT
                value: '8081'
    EOF

It is important the image to be `h2oai/sparkling-water-external-backend:SUBST_SW_VERSION` and not the base H2O image.
Sparkling Water enhances the H2O image with additional dependencies.

For more information about H2O on Kubernetes, please read the
`H2O on Kubernetes Blog Post <https://www.h2o.ai/blog/running-h2o-cluster-on-a-kubernetes-cluster/>`__.

In order for Sparkling Water to be able to connect to the H2O cluster, we need to get the H2O
leader node address. The address is used to specify H2O endpoint during Sparkling Water
configuration phase. This address can be obtained as:

.. code:: bash

    kubectl get endpoints h2o-service -o jsonpath='{.subsets[0].addresses[0].ip}'

After we created the external H2O backend, we can connect to it from Sparkling Water clients as:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Both cluster and client deployment modes of Kubernetes are supported.

    .. tab-container:: Python
        :title: Python

        Both cluster and client deployment modes of Kubernetes are supported.

    .. tab-container:: R
        :title: R

        First, make sure that RSparkling is installed on the node you want to run RSparkling from.
        You can install RSparkling as:

        .. code:: r

           # Download, install, and initialize the H2O package for R.
           # In this case we are using rel-SUBST_H2O_RELEASE_NAME SUBST_H2O_BUILD_NUMBER (SUBST_H2O_VERSION)
           install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/R")

           # Download, install, and initialize the RSparkling
           install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/R")
