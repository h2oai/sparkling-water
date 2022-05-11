Train Deep Learning Model in Sparkling Water
--------------------------------------------

Introduction
~~~~~~~~~~~~

H2O’s Deep Learning is based on a multi-layer feed-forward artificial neural network that is trained with stochastic gradient descent using back-propagation. The network can contain a large number of hidden layers consisting of neurons with tanh, rectifier, and maxout activation functions.
For more more comprehensive description see `H2O-3 Deep learning documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/deep-learning.html>`__.

Example
~~~~~~~

The following section describes how to train the Deep Learning model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2ODeepLearning`
and :ref:`model_details_H2ODeepLearningMOJOModel`.

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
            val datasetUrl = "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/insurance.csv"
            spark.sparkContext.addFile(datasetUrl) //for example purposes, on a real cluster it's better to load directly from distributed storage
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("insurance.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Train the model. You can configure all the available DeepLearning arguments using provided setters, such as the label column
        or the layout of hidden layers.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ODeepLearning
            val estimator = new H2ODeepLearning()
                               .setDistribution("tweedie")
                               .setHidden(Array(1))
                               .setEpochs(1000)
                               .setTrainSamplesPerIteration(-1)
                               .setReproducible(true)
                               .setActivation("Tanh")
                               .setSingleNodeMode(false)
                               .setBalanceClasses(false)
                               .setForceLoadBalance(false)
                               .setSeed(23123)
                               .setTweediePower(1.5)
                               .setScoreTrainingSamples(0)
                               .setColumnsToCategorical("District")
                               .setScoreValidationSamples(0)
                               .setStoppingRounds(0)
                               .setFeaturesCols("District", "Group", "Age")
                               .setLabelCol("Claims")
            val model = estimator.fit(trainingDF)

        By default, the ``H2ODeepLearning`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to worry about
        column data types, you can explicitly specify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2ODeepLearningClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2ODeepLearningRegressor`` instead.

        Eval performance

        .. code:: scala

            val metrics = model.getTrainingMetrics()
            println(metrics)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2ODeepLearningMOJOModel`.


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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/insurance.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Train the model. You can configure all the available Deep Learning arguments using provided setters or constructor parameters,
        such as the label column or the layout of hidden layers.

        .. code:: python

            from pysparkling.ml import H2ODeepLearning
            estimator = H2ODeepLearning(
                            distribution = "tweedie",
                            hidden = [1],
                            epochs = 1000,
                            trainSamplesPerIteration = -1,
                            reproducible = True,
                            activation = "Tanh",
                            singleNodeMode = False,
                            balanceClasses = False,
                            forceLoadBalance = False,
                            seed = 23123,
                            tweediePower = 1.5,
                            scoreTrainingSamples = 0,
                            columnsToCategorical = ["District"],
                            scoreValidationSamples = 0,
                            stoppingRounds = 0,
                            featuresCols = ["District", "Group", "Age"],
                            labelCol = "Claims")
            model = estimator.fit(trainingDF)

        By default, the ``H2ODeepLearning`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to worry about
        column data types, you can explicitly specify the problem by using ``H2ODeepLearningClassifier`` or ``H2ODeepLearningRegressor`` instead.

        Eval performance

        .. code:: python

            metrics = model.getTrainingMetrics()
            print(metrics)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2ODeepLearningMOJOModel`.
