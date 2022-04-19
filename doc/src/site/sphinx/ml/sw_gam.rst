Train GAM Model in Sparkling Water
----------------------------------

**Note**: GAM models are currently experimental.

Introduction
~~~~~~~~~~~~

A generalized additive model (GAM) is a Generalized Linear Model (GLM) in which the linear predictor depends linearly on predictor variables and smooth functions of predictor variables.
For more more comprehensive description see `H2O-3 GAM documentation <https://docs.h2o.ai/h2o/latest-stable/h2o-docs/data-science/gam.html>`__.

Example
~~~~~~~

The following section describes how to train the GAM model in Sparkling Water in Scala & Python following the same example as H2O-3 documentation mentioned above. See also :ref:`parameters_H2OGAM`
and :ref:`model_details_H2OGAMMOJOModel`.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell as

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment

        .. code:: scala

            import ai.h2o.sparkling._
            val hc = H2OContext.getOrCreate()

        Create the frame knots

        .. code:: scala

            val knots1 = Seq(-1.99905699, -0.98143075, 0.02599159, 1.00770987, 1.99942290).toDF()
            val frameKnots1 = hc.asH2OFrame(knots1)
            val knots2 = Seq(-1.999821861, -1.005257990, -0.006716042, 1.002197392, 1.999073589).toDF()
            val frameKnots2 = hc.asH2OFrame(knots2)
            val knots3 = Seq(-1.999675688, -0.979893796, 0.007573327, 1.011437347, 1.999611676).toDF()
            val frameKnots3 = hc.asH2OFrame(knots3)

        Import the dataset and split into train and validation sets

        .. code:: scala

            import org.apache.spark.SparkFiles
            val datasetUrl = "https://s3.amazonaws.com/h2o-public-test-data/smalldata/glm_test/multinomial_10_classes_10_cols_10000_Rows_train.csv"
            spark.sparkContext.addFile(datasetUrl) //for example purposes, on a real cluster it's better to load directly from distributed storage
            val sparkDF =
              spark
                .read
                .option("header", "true")
                .option("inferSchema", "true").csv(SparkFiles.get("multinomial_10_classes_10_cols_10000_Rows_train.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Set the predictor and response columns, specify the knots array

        .. code:: scala

            val y = "C11"
            val x = Array("C1", "C2")

            val numKnots = Array(5, 5, 5)

        Train the model. You can configure all the available GAM arguments using provided setters, such as the label column and gam columns, which are mandatory.

        .. code:: scala

            val gam = new H2OGAM()
              .setFeaturesCols(x)
              .setLabelCol(y)
              .setFamily("multinomial")
              .setGamCols(Array("C6", "C7", "C8"))
              .setColumnsToCategorical("C1", "C2", "C11")
              .setScale(Array(1.0, 1.0, 1.0))
              .setNumKnots(numKnots)
              .setKnotIds(Array(frameKnots1.frameId, frameKnots2.frameId, frameKnots3.frameId))

            val gamModel = gam.fit(trainingDF)

        By default, the ``H2OGAM`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2OGAMClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2OGAMRegressor`` instead.

        Run Predictions

        .. code:: scala

            val predictions = gamModel.transform(testingDF)
            predictions.show(truncate = false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OGAMMOJOModel`.

        Clean up

        .. code:: scala

            frameKnots1.delete()
            frameKnots2.delete()
            frameKnots3.delete()

    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell as

        .. code:: shell

            ./bin/pysparkling

        Start H2O cluster inside the Spark environment

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        Create the frame knots

        .. code:: python

            knots1 = [-1.99905699, -0.98143075, 0.02599159, 1.00770987, 1.99942290]
            frameKnots1 = h2o.H2OFrame(python_obj=knots1)
            knots2 = [-1.999821861, -1.005257990, -0.006716042, 1.002197392, 1.999073589]
            frameKnots2 = h2o.H2OFrame(python_obj=knots2)
            knots3 = [-1.999675688, -0.979893796, 0.007573327,1.011437347, 1.999611676]
            frameKnots3 = h2o.H2OFrame(python_obj=knots3)

        Import the dataset and split into train and validation sets

        .. code:: python

            import h2o
            frame = h2o.import_file("https://s3.amazonaws.com/h2o-public-test-data/smalldata/glm_test/multinomial_10_classes_10_cols_10000_Rows_train.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Set the predictor and response columns, specify the knots array

        .. code:: python

            y = "C11"
            x = ["C1","C2"]

            numKnots = [5,5,5]

        Train the model. You can configure all the available GAM arguments using provided setters or constructor parameters,
        such as the label column and gam columns, which are mandatory.

        .. code:: python

            from pysparkling.ml import H2OGAM

            estimator = H2OGAM(
                featuresCols = x,
                labelCol = y,
                family = "multinomial",
                gamCols = ["C6", "C7", "C8"],
                columnsToCategorical = ["C1", "C2", "C11"],
                scale = [1.0, 1.0, 1.0],
                numKnots = numKnots,
                knotIds = [frameKnots1.key, frameKnots2.key, frameKnots3.key])

            model = estimator.fit(trainingDF)

        By default, the ``H2OGAM`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2OGAMClassifier`` or ``H2OGAMRegressor`` instead.

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OGAMMOJOModel`.

        Clean up

        .. code:: python

            h2o.remove(frameKnots1)
            h2o.remove(frameKnots2)
            h2o.remove(frameKnots3)
            h2o.remove(frame)
