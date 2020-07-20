Train Generalized Low Rank Models (GLRM) in Sparkling Water
-----------------------------------------------------------

Sparkling Water provides API for H2O GLRM in Scala and Python. This algorithm helps to reduce dimensions (a number of features)
of your dataset. For more details about the algorithm, see
`H2O-3 documentation <https://h2o-release.s3.amazonaws.com/h2o/rel-SUBST_H2O_RELEASE_NAME/SUBST_H2O_BUILD_NUMBER/docs-website/h2o-docs/data-science/glrm.html>`__

The following sections describe how to train and apply GLRM in Sparkling Water in both languages.

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
	        val sparkDF = spark.read.option("header", "true")
                .option("inferSchema", "true")
	            .csv(SparkFiles.get("iris_wheader.csv"))
                .drop("class")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available GLRM arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGLRM
            val estimator = new H2OGLRM()
                .setK(3) // Number of output dimensions
                .setLoss("quadratic")
                .setRepresentationName("myXFrame")
                .setGammaX(0.5)
                .setGammaY(0.5)
                .setTransform("standardize")
            val model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: scala

            model.getModelDetails()

        Transform your dataset, the output will be under the ``prediction`` column.

        .. code:: scala

            model.transform(testingDF).show(false)

        If you want to get the resulting X matrix, set a custom name for a H2O Frame representing the matrix
        via ``setRepresentationName`` method and run the following:

        .. code:: scala

            import ai.h2o.sparkling.H2OFrame
            val xDataFrame = hc.asSparkFrame(H2OFrame("myXFrame"))
            xDataFrame.show(false)


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
            sparkDF = hc.asSparkFrame(frame).drop("class")
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available GLRM arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OGLRM
            estimator = H2OGLRM(k=3, loss="quadratic", gammaX=0.5, gammaY=0.5, transform="standardize", representationName="myXFrame")
            model = estimator.fit(trainingDF)

        You can also get raw model details by calling the *getModelDetails()* method available on the model as:

        .. code:: python

            model.getModelDetails()

        Transform your dataset, the output will be under the ``prediction`` column.

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        If you want to get the resulting X matrix, set a custom name for a H2O Frame representing the matrix
        via ``setRepresentationName`` method or the corresponding parameter and run the following:

        .. code:: python

            from h2o.frame import H2OFrame
            xDataFrame = hc.asSparkFrame(H2OFrame.get_frame("myXFrame", full_cols=-1, light=True))
            xDataFrame.show(truncate=False)
