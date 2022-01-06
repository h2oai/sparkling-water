Target Encoding in Sparkling Water
==================================
Target Encoding in Sparkling Water is a mechanism of converting categorical features to continues features based on
the mean calculated from values of the label (target) column. See also :ref:`parameters_H2OTargetEncoder`.

An example of converting a categorical feature to continues with Target Encoder (`Town_te` is a produced column):

 =============== ======= =========
  Town            Label   Town_te  
 =============== ======= ========= 
  Chennai         1       0.8      
  Prague          0       0.286    
  Chennai         0       0.8      
  Mountain View   1       0.714    
  Chennai         1       0.8      
  Prague          1       0.286    
  Mountain View   1       0.714    
  Chennai         1       0.8      
  Mountain View   0       0.714    
  Prague          1       0.286    
  Prague          0       0.286    
  Mountain View   1       0.714    
  Prague          0       0.286    
  Mountain View   0       0.714    
  Chennai         1       0.8
  Mountain View   1       0.714    
  Prague          0       0.286    
  Prague          0       0.286    
  Mountain View   1       0.714    
 =============== ======= =========

Target Encoding can help to improve accuracy of machine learning algorithms when columns with high
cardinality are used as features during a training phase.

Using Target Encoder
--------------------
Sparkling Water exposes API for target encoder in Scala and Python. Before we start using Target Encoder, we need to run
and prepare the environment:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        First, let's start Sparkling Shell (use *:paste* mode when you try to copy-paste examples):

        .. code:: shell

            ./bin/sparkling-shell

        Start H2O cluster inside the Spark environment:

        .. code:: scala

            import ai.h2o.sparkling._
            import java.net.URI
            val hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame:

        .. code:: scala

            import org.apache.spark.SparkFiles
            spark.sparkContext.addFile("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            val sparkDF = spark.read.option("header", "true").option("inferSchema", "true").csv(SparkFiles.get("prostate.csv"))
            val Array(trainingDF, testingDF) = sparkDF.randomSplit(Array(0.8, 0.2))

    .. tab-container:: Python
        :title: Python

        First, let's start PySparkling Shell:

        .. code:: shell

            ./bin/pysparkling

        Start H2O cluster inside the Spark environment:

        .. code:: python

            from pysparkling import *
            hc = H2OContext.getOrCreate()

        Parse the data using H2O and convert them to Spark Frame:

        .. code:: python

            import h2o
            frame = h2o.import_file("https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv")
            sparkDF = hc.asSparkFrame(frame)
            [trainingDF, testingDF] = sparkDF.randomSplit([0.8, 0.2])


Target Encoder in ML Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Target Encoder in Sparkling Water is implemented as a regular estimator and thus could be placed as a stage to Spark ML Pipeline

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        Let's create an instance of Target Encoder and configure it:

        .. code:: scala

            import ai.h2o.sparkling.ml.features.H2OTargetEncoder
            val targetEncoder = new H2OTargetEncoder()
              .setInputCols(Array("RACE", "DPROS", "DCAPS"))
              .setProblemType("Classification")
              .setLabelCol("CAPSULE")

        Also, create an instance of an algorithm consuming encoded columns and define pipeline:

        .. code:: scala

            import ai.h2o.sparkling.ml.algos.classification.H2OGBMClassifier
            import org.apache.spark.ml.Pipeline
            val gbm = new H2OGBMClassifier()
                .setFeaturesCols(targetEncoder.getOutputCols())
                .setLabelCol("CAPSULE")
            val pipeline = new Pipeline().setStages(Array(targetEncoder, gbm))

        Train the created pipeline

        .. code:: scala

            val pipelineModel = pipeline.fit(trainingDF)

        Make predictions including a model of Target Encoder:

        .. code:: scala

            pipelineModel.transform(testingDF).show()

        The model of Target Encoder is persistable to MOJO, so you can save and load the whole pipeline model:

        .. code:: scala

            import org.apache.spark.ml.PipelineModel
            pipelineModel.write.save("somePathForStoringPipelineModel")
            val loadedPipelineModel = PipelineModel.load("somePathForStoringPipelineModel")
            loadedPipelineModel.transform(testingDF).show()

    .. tab-container:: Python
        :title: Python

        Let's create an instance of Target Encoder and configure it:

        .. code:: python

            from pysparkling.ml import H2OTargetEncoder
            targetEncoder = H2OTargetEncoder()\
              .setInputCols(["RACE", "DPROS", "DCAPS"])\
              .setLabelCol("CAPSULE")\
              .setProblemType("Classification")

        Also, create an instance of an algorithm consuming encoded columns and define pipeline:

        .. code:: python

            from pysparkling.ml import H2OGBMClassifier
            from pyspark.ml import Pipeline
            gbm = H2OGBMClassifier()\
                .setFeaturesCols(targetEncoder.getOutputCols())\
                .setLabelCol("CAPSULE")
            pipeline = Pipeline(stages=[targetEncoder, gbm])

        Train the created pipeline

        .. code:: python

            pipelineModel = pipeline.fit(trainingDF)

        Make predictions including a model of Target Encoder:

        .. code:: python

            pipelineModel.transform(testingDF).show()

        The model of Target Encoder is persistable to MOJO, so you can save and load the whole pipeline model:

        .. code:: python

            from pyspark.ml import PipelineModel
            pipelineModel.save("somePathForStoringPipelineModel")
            loadedPipelineModel = PipelineModel.load("somePathForStoringPipelineModel")
            loadedPipelineModel.transform(testingDF).show()


Standalone Target Encoder
~~~~~~~~~~~~~~~~~~~~~~~~~
Target Encoder's parameters like ``noise`` and ``holdoutStrategy`` are relevant only for a training dataset.
Thus the ``transform`` method of ``H2OTargetEncoderModel`` has to treat training and other data sets differently and
eventually, ignore the mentioned parameters.

When Target Encoder is inside a ML pipeline, the differentiation is done automatically. But if a user decides to train
an algorithm without ML pipeline, the 'transformTrainingDataset' method should be on the model of Target Encoder to get
appropriate results.


Edge Cases
~~~~~~~~~~
- The label column can't contain any ``null`` values.
- Input columns transformed by Target Encoder can contain ``null`` values.
- Novel values in a testing/production data set and ``null`` values belong to the same category. In other words,
  Target Encoder returns a prior average for all novel values in case a given column of the training dataset
  did not contain any ``null`` values. Otherwise, the posterior average of rows having ``null`` values in the column is returned.
