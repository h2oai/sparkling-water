Train KMeans Model in Sparkling Water
--------------------------------------

Sparkling Water provides API for H2O KMeans in Scala and Python.
The following sections describe how to train KMeans model in Sparkling Water in both languages.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment

        .. code:: scala

            import org.apache.spark.h2o._
            import java.net.URI
            val hc = H2OContext.getOrCreate(spark)

        Parse the data using H2O and convert them to Spark Frame

        .. code:: scala

            val frame = new H2OFrame(new URI("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv"))
            val sparkDF = hc.asDataFrame(frame)
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available KMeans arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OKMeans
            val estimator = new H2OKMeans().setK(2).setUserPoints(Array(Array(4.9, 3.0, 1.4, 0.2, 0), Array(5.6, 2.5, 3.9, 1.1, 1)))
            val model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: scala

            model.getModelDetails()

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)


    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        Start H2O cluster inside the Spark environment

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate(spark)

        Parse the data using H2O and convert them to Spark Frame

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv")
            sparkDF = hc.as_spark_frame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available KMeans arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2OKMeans
            estimator = H2OKMeans(k=3, userPoints=[[4.9, 3.0, 1.4, 0.2, 0], [5.6, 2.5, 3.9, 1.1, 1], [6.5, 3.0, 5.2, 2.0, 2]])
            model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

