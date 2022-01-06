.. _coxph:

Train CoxPH Model in Sparkling Water
-----------------------------------------------

Sparkling Water provides API for H2O CoxPH in Scala and Python.
The following sections describe how to train the CoxPH model in Sparkling Water in both languages.
See also :ref:`parameters_H2OCoxPH` and :ref:`model_details_H2OCoxPHMOJOModel`.

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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/coxph_test/heart.csv")
            val heartDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("heart.csv"))
            val Array(trainingDF, testingDF) = heartDF.randomSplit(Array(0.8, 0.2), seed = 12345)

        Train the model. You can configure all the available CoxPH parameters using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OCoxPH
            val estimator = new H2OCoxPH().
                setStartCol("start").
                setStopCol("stop").
                setTies("breslow").
                setLabelCol("event")
            val model = estimator.fit(trainingDF)

        Run Predictions

        .. code:: scala

            model.transform(testingDF).show(false)

        You can also get model details via calling methods listed in :ref:`model_details_H2OCoxPHMOJOModel`.


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
            heartFrame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/coxph_test/heart.csv")
            trainingFrame, testingFrame = heartFrame.split_frame(ratios = [.8], seed = 1234)
            trainingDF = hc.asSparkFrame(trainingFrame)
            testingDF = hc.asSparkFrame(testingFrame)

        Train the model. You can configure all the available CoxPH arguments using provided setters or constructor parameters.

        .. code:: python

            from pysparkling.ml import H2OCoxPH
            estimator = H2OCoxPH()\
                .setStartCol('start')\
                .setStopCol('stop')\
                .setTies('breslow')\
                .setLabelCol('event')
            model = estimator.fit(trainingDF)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)

        You can also get model details via calling methods listed in :ref:`model_details_H2OCoxPHMOJOModel`.

