Train Word2Vec Model in Sparkling Water
---------------------------------------

Sparkling Water provides API for H2O Word2Vec in Scala and Python.
The following sections describe how to train the Word2Vec model in Sparkling Water in both languages.
See also :ref:`parameters_H2OWord2Vec`.

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
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/craigslistJobTitles.csv")
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("craigslistJobTitles.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

        Create the pipeline with the H2O Word2Vec. You can configure all the available Word2Vec arguments using provided setters.

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.H2OGBM
            import ai.h2o.sparkling.ml.features.H2OWord2Vec
            import org.apache.spark.ml.Pipeline
            import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}

            val tokenizer = new RegexTokenizer()
              .setInputCol("jobtitle")
              .setMinTokenLength(2)

            val stopWordsRemover = new StopWordsRemover()
              .setInputCol(tokenizer.getOutputCol)

            val w2v = new H2OWord2Vec()
              .setSentSampleRate(0)
              .setEpochs(10)
              .setInputCol(stopWordsRemover.getOutputCol)

            val gbm = new H2OGBM()
              .setLabelCol("category")
              .setFeaturesCols(w2v.getOutputCol)

            val pipeline = new Pipeline().setStages(Array(tokenizer, stopWordsRemover, w2v, gbm))

        Train the pipeline:

        .. code:: scala

            val model = pipeline.fit(trainingDF)

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
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/craigslistJobTitles.csv")
            sparkDF = hc.asSparkFrame(frame.set_names(['category', 'jobtitle']))
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])

        Create the pipeline with the Word2Vec. You can configure all the available Word2Vec arguments using provided setters.

        .. code:: python

            from pysparkling.ml import H2OGBM, H2OWord2Vec
            from pyspark.ml import Pipeline
            from pyspark.ml.feature import RegexTokenizer, StopWordsRemover

            tokenizer = RegexTokenizer(inputCol="jobtitle", minTokenLength=2)
            stopWordsRemover = StopWordsRemover(inputCol=tokenizer.getOutputCol())
            w2v = H2OWord2Vec(sentSampleRate=0, epochs=10, inputCol=stopWordsRemover.getOutputCol())
            gbm = H2OGBM(labelCol="category", featuresCols=[w2v.getOutputCol()])

            pipeline = Pipeline(stages=[tokenizer, stopWordsRemover, w2v, gbm])

        Train the pipeline:

        .. code:: python

            model = pipeline.fit(trainingDF)

        Run Predictions

        .. code:: python

            model.transform(testingDF).show(truncate = False)
