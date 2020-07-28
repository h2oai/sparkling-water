Deploying PySparkling Pipeline Models
-------------------------------------

This tutorial demonstrates how we can import PySparkling pipeline models for scoring.

Let's first create and export the model as:

.. code:: python

    from pyspark.ml import Pipeline
    from pysparkling import *
    from pysparkling.ml import *

    hc = H2OContext.getOrCreate()

    # Helper method to locate the data file
    def locate(file_name):
        return "examples/smalldata/smsData.txt"

    # Prepare the data
    def load():
        row_rdd = spark.sparkContext.textFile(locate("smsData.txt")).map(lambda x: x.split("\t", 1)).filter(lambda r: r[0].strip())
        return spark.createDataFrame(row_rdd, ["label", "text"])

    # load the data
    data = load()

    # Create the H2O GBM pipeline stage
    gbm = H2OGBM(splitRatio=0.8, seed=1, labelCol="label")

    # Create a pipeline with a single GBM step
    pipeline = Pipeline(stages=[gbm])

    # Fit and export the pipeline
    model = pipeline.fit(data)
    model.save("exported_model")


Once we have exported the model, let's start a new ``./pysparkling`` shell as we want to demonstrate that for scoring,
H2OContext does not need to be created as the ``H2OGBM`` step is internally using MOJO which does not require run-time of H2O.


First, we need to ensure that all Java classes internally stored in the PySparkling distribution are distributed in the Spark
cluster. For that, we use the following code:

.. code:: python

    from pysparkling.initializer import Initializer
    Initializer.load_sparkling_jar(spark)

Once we initialized PySparkling, we can load the model as:

.. code:: python

    from pyspark.ml import PipelineModel
    model = PipelineModel.load("exported_model")

And we can run the predictions on the model as:

.. code:: python

    df_for_predictions = ..
    model.transform(df_for_predictions)

If we don't initialize the PySparkling using the ``Initializer``, we would get ``class not found`` exception during loading
the model as Spark would not know about the required classes. But as we can see, we do not need to initialize ``H2OContext``
for scoring tasks.
