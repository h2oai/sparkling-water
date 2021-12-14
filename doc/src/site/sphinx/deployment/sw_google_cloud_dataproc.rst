Running Sparkling Water on Google Cloud Dataproc
------------------------------------------------

This section describes how to run Sparkling Water on `Google Cloud Dataproc <https://cloud.google.com/dataproc/docs/concepts/overview>`__. 
It is meant to get you up and running with Sparkling Water on Google Cloud Dataproc as fast as possible so you can try it out.
For further usage and productionizing some adjustments like `initialization actions <https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/init-actions>`__ will be required.
In this tutorial we will use Dataproc image version 2.0-debian10 which has Spark 3.1 and Scala 2.12.

1. Install the `Google Cloud SDK <https://cloud.google.com/sdk/docs/install>`__. Login to your account.
2. Download `Sparkling water <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.1/latest.html>`__.
3. Create a Google Cloud Dataproc cluster.

.. code:: bash

    DATAPROC_CLUSTER_NAME='sparkling-water-test'
    GCP_REGION='europe-central2'

    gcloud dataproc clusters create $DATAPROC_CLUSTER_NAME \
    --region $GCP_REGION \
    --image-version 2.0-debian10 \
    --num-workers 3 \
    --properties='^#^dataproc:pip.packages=tabulate==0.8.3,requests==2.21.0,future==0.17.1'

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Set the variables.

        .. code:: bash

            # we need spark jars to compile the example
            SPARK_JARS=$(echo "$SPARK_HOME"/jars/*.jar | tr ' ' ':')
            SPARKLING_WATER_JAR='sparkling-water-assembly_2.12-SUBST_SW_VERSION-all.jar'

        Copy the example job source into a file named SparklingWaterGcpExampleJob.scala

        .. code:: scala

            import java.net.URI
            import ai.h2o.sparkling._
            import org.apache.spark.SparkFiles
            import org.apache.spark.sql.SparkSession

            object SparklingWaterGcpExampleJob extends App {
              // start the cluster
              val spark = SparkSession.builder.appName("Sparkling water example").getOrCreate()
              import spark.implicits._
              val hc = H2OContext.getOrCreate()

              val expectedClusterSize = 3
              val clusterSize = hc.getH2ONodes().length
              require(clusterSize != expectedClusterSize, s"H2O cluster should be of size $expectedClusterSize but is $clusterSize")

              // prepare the data
              spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
              val frame = H2OFrame(new URI("file://" + SparkFiles.get("prostate.csv")))
              val sparkDF = hc.asSparkFrame(frame).withColumn("CAPSULE", $"CAPSULE" cast "string")
              val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

              // train the model
              import ai.h2o.sparkling.ml.algos.H2OGBM
              val estimator = new H2OGBM().setLabelCol("CAPSULE")
              val model = estimator.fit(trainingDF)

              // run predictions
              model.transform(testingDF).collect()
            }

        Compile the code into a jar file having Spark and Sparkling Water on the classpath.

        .. code:: bash

            EXAMPLE_JOB_JAR='sparkling-water-gcp-example-job.jar'
            scalac SparklingWaterGcpExampleJob.scala \
                  -d $EXAMPLE_JOB_JAR \
                  -classpath "$SPARKLING_WATER_JAR:$SPARK_JARS"

        Submit the job to the cluster.

        .. code:: bash

            gcloud dataproc jobs submit spark \
            --class=SparklingWaterGcpExampleJob \
            --cluster=$DATAPROC_CLUSTER_NAME \
            --region=$GCP_REGION \
            --jars "$SPARKLING_WATER_JAR,$EXAMPLE_JOB_JAR" \
            --properties=spark.dynamicAllocation.enabled=false,spark.scheduler.minRegisteredResourcesRatio=1,spark.executor.instances=3


    .. tab-container:: Python
        :title: Python

        Set the variables.

        .. code:: bash

            PYSPARKLING_ZIP='h2o_pysparkling_3.1-SUBST_SW_VERSION.zip'

        Copy the example job source into a file named sparkling_water_gcp_example_job.py

        .. code:: python

            from pysparkling import *
            from pyspark.sql import SparkSession
            import h2o

            # start the cluster
            spark = SparkSession.builder.appName("Sparkling water example").getOrCreate()
            hc = H2OContext.getOrCreate()
            assert h2o.cluster().cloud_size == 3

            # prepare the data
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

            # train the model
            from pysparkling.ml import H2OGBM
            estimator = H2OGBM(labelCol = "CAPSULE")
            model = estimator.fit(trainingDF)

            # run predictions
            model.transform(testingDF).collect()

        Submit the job to the cluster.

        .. code:: bash

            gcloud dataproc jobs submit pyspark sparkling_water_gcp_example_job.py \
            --cluster=$DATAPROC_CLUSTER_NAME \
            --region=$GCP_REGION \
            --py-files $PYSPARKLING_ZIP \
            --properties=spark.dynamicAllocation.enabled=false,spark.scheduler.minRegisteredResourcesRatio=1,spark.executor.instances=3
