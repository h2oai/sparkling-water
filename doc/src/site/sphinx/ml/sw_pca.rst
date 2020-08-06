Train Principal Component Analysis Model (PCA) in Sparkling Water
-----------------------------------------------------------------

Sparkling Water provides API for H2O PCA in Scala and Python. This algorithm helps to reduce dimensions (a number of features)
of your dataset. For more details about the algorithm, see
`H2O-3 documentation <https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/data-science/pca.html>`__

The following sections describe how to train and apply PCA in Sparkling Water in both languages. See also :ref:`parameters_H2OPCA`.

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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/birds.csv")
	        val sparkDF = spark.read.option("header", "true")
                .option("inferSchema", "true")
	            .csv(SparkFiles.get("birds.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available PCA arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OPCA
            val estimator = new H2OPCA()
                .setK(3) // Number of output dimensions
                .setMaxIterations(500)
                .setPcaImpl("MTJ_EVD_SYMMMATRIX")
                .setPcaMethod("GramSVD")
            val model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: scala

            model.getModelDetails()

        Transform your dataset, the output will be under the ``prediction`` column.

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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/birds.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available PCA arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OPCA
            estimator = H2OGPCA(k=3, maxIterations="500", pcaImpl="MTJ_EVD_SYMMMATRIX", pcaMethod="GramSVD")
            model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Transform your dataset, the output will be under the ``prediction`` column.

        .. code:: python

            model.transform(testingDF).show(truncate = False)
