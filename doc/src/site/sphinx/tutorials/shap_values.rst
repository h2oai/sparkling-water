Obtain SHAP values from MOJO model
----------------------------------

You can train the pipeline in Sparkling Water and get contributions from it or you can also
get contributions from raw mojo. The following two sections describe how to achieve that.

Train model pipeline & get contributions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obtaining SHAP values is possible only from H2OGBM, H2OXGBoost and H2ODRF pipeline wrappers and for
regression or binomial problems.

To get SHAP values(=contributions) from H2OXGBoost model, please do:

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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
	        val rawSparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val sparkDF = rawSparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available XGBoost arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OXGBoost
            val estimator = new H2OXGBoost()
                .setLabelCol("CAPSULE")
                .setWithDetailedPredictionCol(true)
                .setWithContributions(true)
            val model = estimator.fit(trainingDF)

        The call ``setWithDetailedPredictionCol(true)`` tells Sparkling Water to create additional prediction column with
        additional prediction details and the call ``setWithContributions(true)`` tells to include contributions to
        this column. The name of this column is by default "detailed_prediction" and can be modified via
        ``setDetailedPredictionCol`` setter.

        Run Predictions

        .. code:: scala

            val predictions = model.transform(testingDF).show(false)

        Show contributions

        .. code:: scala

            predictions.select("detailed_prediction.contribution").show()



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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            sparkDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available XGBoost arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2OXGBoost
            estimator = H2OXGBoost(labelCol = "CAPSULE", withDetailedPredictionCol = True, withContributions = True)
            model = estimator.fit(trainingDF)

        The parameter ``withDetailedPredictionCol = True`` tells Sparkling Water to create an additional prediction column with
        additional prediction details and the parameter ``withContributions = True`` tells to include contributions to this column.
        The name of this column is by default "detailed_prediction" and can be modified via ``detailedPredictionCol`` parameter.

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        Show contributions

        .. code:: python

            predictions.select("detailed_prediction.contributions").show()

Get Contributions from Raw MOJO
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Obtaining SHAP values is possible only from MOJO's generated for GBM, XGBoost and DRF and for
regression or binomial problems. If you don't need to train the model and just need to load existing mojo,
there is no need to start ``H2OContext``.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Parse the data using Spark

        .. code:: scala

            val testingDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/path/to/testing/dataset.csv")

        Load the existing MOJO and enable generation of contributions via the settings object.

        .. code:: scala

            import ai.h2o.sparkling.ml.models._

            val path = "/path/to/mojo.zip"
            val settings = H2OMOJOSettings(withDetailedPredictionCol = true)
            val model = H2OMOJOModel.createFromMojo(path, settings)

        Run Predictions

        .. code:: scala

            val predictions = model.transform(testingDF)

        Show contributions

        .. code:: scala

            predictions.select("detailed_prediction.contributions").show()



    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        Parse the data using Spark

        .. code:: python

            testingDF = spark.read.csv("/path/to/testing/dataset.csv", header=True, inferSchema=True)

        Load the existing MOJO and enable generation of contributions via the settings object.

        .. code:: python

            from pysparkling.ml import *

            val path = '/path/to/mojo.zip'
            settings = H2OMOJOSettings(withDetailedPredictionCol=True)
            model = H2OMOJOModel.createFromMojo(path, settings)

        Run Predictions

        .. code:: python

            val predictions = model.transform(testingDF)

        Show contributions

        .. code:: python

            predictions.select("detailed_prediction.contributions").show()
