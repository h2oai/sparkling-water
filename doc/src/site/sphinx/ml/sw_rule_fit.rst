.. _rule_fit:

Train RuleFit Model in Sparkling Water
--------------------------------------

RuleFit algorithm combines tree ensembles and linear models to take advantage of both methods:
- the accuracy of a tree ensemble
- the interpretability of a linear model

The general algorithm fits a tree ensemble to the data, builds a rule ensemble by traversing each tree, evaluates the rules on
the data to build a rule feature set, and fits a sparse linear model (LASSO) to the rule feature set joined with the original feature set.

Sparkling Water provides API for H2O RuleFit in Scala and Python. The following sections describe how to train the RuleFit model
in Sparkling Water in both languages. See also :ref:`parameters_H2ORuleFit` and :ref:`model_details_H2ORuleFitMOJOModel`.

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
	        val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val trainingDF = sparkDF.withColumn("CAPSULE", $"CAPSULE" cast "string")

        Train the model. You can configure all the available RuleFit arguments using provided setters, such as the label column.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2ORuleFit
            val estimator = new H2ORuleFit().setLabelCol("CAPSULE")
            val model = estimator.fit(trainingDF)

        By default, the ``H2ORuleFit`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want be worried about
        column data types, you can explicitly identify the problem by using ``ai.h2o.sparkling.ml.algos.classification.H2OH2ORuleFitClassifier``
        or ``ai.h2o.sparkling.ml.algos.regression.H2OH2ORuleFitRegressor`` instead.

        Get trained rules

        .. code:: scala

            model.getRuleImportance().show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2ORuleFitMOJOModel`.


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
            trainingDF = sparkDF.withColumn("CAPSULE", sparkDF.CAPSULE.cast("string"))

        Train the model. You can configure all the available RuleFit arguments using provided setters or constructor parameters, such as the label column.

        .. code:: python

            from pysparkling.ml import H2ORuleFit
            estimator = H2ORuleFit(labelCol = "CAPSULE")
            model = estimator.fit(trainingDF)

        By default, the ``H2ORuleFit`` algorithm distinguishes between a classification and regression problem based on the type of
        the label column of the training dataset. If the label column is a string column, a classification model will be trained.
        If the label column is a numeric column, a regression model will be trained. If you don't want to be worried about
        column data types, you can explicitly identify the problem by using ``H2ORuleFitClassifier`` or ``H2ORuleFitRegressor`` instead.

        Get trained rules

        .. code:: python

            model.getRuleImportance().show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2ORuleFitMOJOModel`.
