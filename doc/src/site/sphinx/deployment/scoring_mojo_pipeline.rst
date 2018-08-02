Using the MOJO Scoring Pipeline with Spark/Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MOJO scoring pipeline artifacts can be used in Spark to deploy predictions in parallel using the Sparkling Water API. This section shows how to load and run predictions on the MOJO scoring pipeline in Spark using Scala and the Python API.

In the event that you upgrade H2O Driverless AI, we have a good news! Sparkling Water is backwards compatible with MOJO versions produced by older Driverless AI versions.

Requirements
''''''''''''

- You must have a Spark cluster with the Sparkling Water JAR file passed to Spark.
- To run with PySparkling, you must have the PySparkling zip file.

The H2OContext does not have to be created if you only want to run predictions on MOJOs using Spark. This is because they are written to be independent of the H2O run-time.

Preparing Your Environment
''''''''''''''''''''''''''

Both PySparkling and Sparkling Water need to be started with some extra configurations in order to enable the MOJO scoring pipeline. Examples are provided below. Specifically, you must pass the path of the H2O Driverless AI license to the Spark ``--jars`` argument. Additionally, you need to add to the same ``--jars`` configuration path to the MOJO scoring pipeline implementation JAR file ``mojo2-runtime.jar``. This file is propriatory and is not part of the resulting Sparkling Water assembly JAR file.

**Note**: In Local Spark mode, please use ``--driver-class-path`` to specify path to the license file and the MOJO Pipeline JAR file.

PySparkling
'''''''''''

First, start PySpark with all the required dependencies. The following command passes the license file and the MOJO scoring pipeline implementation library to the
``--jars`` argument and also specifies the path to the PySparkling Python library.

.. code:: bash

    ./bin/pyspark --jars license.sig,mojo2-runtime.jar --py-files pysparkling.zip

or, you can download official Sparkling Water distribution from `H2O Download page <https://www.h2o.ai/download/>`__. Please follow steps on the
Sparkling Water download page. Once you are in the Sparkling Water directory, you can call:

.. code:: bash

    ./bin/pysparkling --jars license.sig,mojo2-runtime.jar


At this point, you should have available a PySpark interactive terminal where you can try out predictions. If you would like to productionalize the scoring process, you can use the same configuration, except instead of using ``./bin/pyspark``, you would use ``./bin/spark-submit`` to submit your job to a cluster.

.. code:: python

	# First, specify the dependency
	from pysparkling.ml import H2OMOJOPipelineModel

.. code:: python

	# Load the pipeline
	mojo = H2OMOJOPipelineModel.create_from_mojo("file:///path/to/the/pipeline.mojo")

	# This option ensures that the output columns are named properly. If you want to use old behavior
	# when all output columns were stored inside an array, don't specify this configuration option,
	# or set it to False. We however strongly encourage users to set this to True as below.
	mojo.set_named_mojo_output_columns(True)

.. code:: python

	# Load the data as Spark's Data Frame
	data_frame = spark.read.csv("file:///path/to/the/data.csv", header=True)

.. code:: python

	# Run the predictions. The predictions contain all the original columns plus the predictions
	# added as new columns
	predictions = mojo.predict(data_frame)

	# You can easily get the predictions for a desired column using the helper function as
	predictions.select(mojo.select_prediction_udf("AGE")).collect()

Sparkling Water
'''''''''''''''

First start Spark with all the required dependencies. The following command passes the license file and the MOJO scoring pipeline implementation library
``mojo2-runtime.jar`` to the ``--jars`` argument and also specifies the path to the Sparkling Water assembly jar.

.. code:: bash

    ./bin/spark-shell --jars license.sig,mojo2-runtime.jar,sparkling-water-assembly.jar

At this point, you should have available a Sparkling Water interactive terminal where you can try out predictions. If you would like to productionalize the scoring process, you can use the same configuration, except instead of using ``./bin/spark-shell``, you would use ``./bin/spark-submit`` to submit your job to a cluster.

.. code:: scala

	// First, specify the dependency
	import org.apache.spark.ml.h2o.models.H2OMOJOPipelineModel

.. code:: scala

	// Load the pipeline
	val mojo = H2OMOJOPipelineModel.createFromMojo("file:///path/to/the/pipeline.mojo")

	// This option ensures that the output columns are named properly. If you want to use old behaviour
	// when all output columns were stored inside and array, don't specify this configuration option
	// or set it to False. We however strongly encourage users to set this to true as below.
	mojo.setNamedMojoOutputColumns(true)

.. code:: scala

    // Load the data as Spark's Data Frame
    val dataFrame = spark.read.option("header", "true").csv("file:///path/to/the/data.csv")

.. code:: scala

	// Run the predictions. The predictions contain all the original columns plus the predictions
	// added as new columns
	val predictions = mojo.transform(dataFrame)

	// You can easily get the predictions for desired column using the helper function as follows:
	predictions.select(mojo.selectPredictionUDF("AGE"))
