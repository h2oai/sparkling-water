Train KMeans Model in Sparkling Water
--------------------------------------

Sparkling Water provides API for H2O KMeans in Scala and Python. All available parameters
of H2OKmeans model are described at `H2O KMeans Parameters`_.

The following sections describe how to train KMeans model in Sparkling Water in both languages.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment

        .. code:: scala

            import ai.h2o.sparkling._
            import java.net.URI
            val hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: scala

	        import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv")
	        val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("iris_wheader.csv"))
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
            hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/iris/iris_wheader.csv")
            sparkDF = hc.asSparkFrame(frame)
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

H2O KMeans Parameters
~~~~~~~~~~~~~~~~~~~~~

- **maxIterations**
    Maximum number of KMeans iterations to find the centroids.
- **standardize**
    Standardize the numeric columns to have a mean of zero and unit variance.  More information about
    the standardization is available at `H2O KMeans standardize param documentation <https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/data-science/algo-params/standardize.html>`__.
- **init**
    Initialization mode for finding the initial cluster centers. More information about
    the initialization is available at `H2O KMeans Init param documentation <https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/data-science/algo-params/init.html>`__.
- **userPoints**
    This option allows you to specify array of points, where each point represents
    coordinates of an initial cluster center. The user-specified points must have the same number of columns as the training observations.
    The number of rows must equal the number of clusters.
- **estimateK**
    If enabled, the algorithm tries to identify an optimal number of clusters, up to **k** clusters.
- **k**
    Number of clusters to generate.

