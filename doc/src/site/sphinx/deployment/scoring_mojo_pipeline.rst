Using the MOJO Scoring Pipeline with Spark/Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

MOJO scoring pipeline artifacts can be used in Spark to carry out predictions in parallel
using the Sparkling Water API. This section shows how to load and run predictions on the
MOJO scoring pipeline in Spark using Scala and the Python API.

**Note**: Sparkling Water is backwards compatible with MOJO versions produced by different Driverless AI versions.

Requirements
''''''''''''

- You must have a Spark cluster with the Sparkling Water JAR file passed to Spark.
- To run with PySparkling, you must have the PySparkling zip file.

The H2OContext does not have to be created if you only want to run predictions on MOJOs using Spark.
This is because the scoring is independent of the H2O run-time.

Preparing Your Environment
''''''''''''''''''''''''''
In order use the MOJO scoring pipeline, Driverless AI license has to be passed to Spark.
This can be achieved via ``--jars`` argument of the Spark launcher scripts.

**Note**: In Local Spark mode, please use ``--driver-class-path`` to specify path to the license file.

PySparkling
'''''''''''

First, start PySpark with PySparkling Python package and Driverless AI license.

.. code:: bash

    ./bin/pyspark --jars license.sig --py-files pysparkling.zip

or, you can download official Sparkling Water distribution from `H2O Download page <https://www.h2o.ai/download/>`__. Please follow steps on the
Sparkling Water download page. Once you are in the Sparkling Water directory, you can call:

.. code:: bash

    ./bin/pysparkling --jars license.sig


At this point, you should have available a PySpark interactive terminal where you can try out predictions. If you would like to productionalize the scoring process, you can use the same configuration, except instead of using ``./bin/pyspark``, you would use ``./bin/spark-submit`` to submit your job to a cluster.

.. code:: python

	# First, specify the dependency
	from pysparkling.ml import H2OMOJOPipelineModel

.. code:: python

	# The 'namedMojoOutputColumns' option ensures that the output columns are named properly.
	# If you want to use old behavior when all output columns were stored inside an array,
	# set it to False. However we strongly encourage users to use True which is defined as a default value.
	settings = H2OMOJOSettings(namedMojoOutputColumns = True)

	# Load the pipeline. 'settings' is an optional argument. If it's not specified, the default values are used.
	mojo = H2OMOJOPipelineModel.createFromMojo("file:///path/to/the/pipeline.mojo", settings)

.. code:: python

	# Load the data as Spark's Data Frame
	dataFrame = spark.read.csv("file:///path/to/the/data.csv", header=True)

.. code:: python

	# Run the predictions. The predictions contain all the original columns plus the predictions
	# added as new columns
	predictions = mojo.transform(dataFrame)

	# You can easily get the predictions for a desired column using the helper function as
	predictions.select(mojo.selectPredictionUDF("AGE")).collect()

Sparkling Water
'''''''''''''''

First, start Spark with Sparkling Water Scala assembly and Driverless AI license.

.. code:: bash

    ./bin/spark-shell --jars license.sig,sparkling-water-assembly.jar

or, you can download official Sparkling Water distribution from `H2O Download page <https://www.h2o.ai/download/>`__. Please follow steps on the
Sparkling Water download page. Once you are in the Sparkling Water directory, you can call:

.. code:: bash

    ./bin/sparkling-shell --jars license.sig


At this point, you should have available a Sparkling Water interactive terminal where you can carry out predictions. If you would like to productionalize the scoring process, you can use the same configuration, except instead of using ``./bin/spark-shell``, you would use ``./bin/spark-submit`` to submit your job to a cluster.

.. code:: scala

	// First, specify the dependency
	import ai.h2o.sparkling.ml.models.H2OMOJOPipelineModel

.. code:: scala

	// The 'namedMojoOutputColumns' option ensures that the output columns are named properly.
	// If you want to use old behavior when all output columns were stored inside an array,
	// set it to false. However we strongly encourage users to use true which is defined as a default value.
	val settings = H2OMOJOSettings(namedMojoOutputColumns = true)

	// Load the pipeline. 'settings' is an optional argument. If it's not specified, the default values are used.
	val mojo = H2OMOJOPipelineModel.createFromMojo("file:///path/to/the/pipeline.mojo", settings)

.. code:: scala

	// Load the data as Spark's Data Frame
	val dataFrame = spark.read.option("header", "true").csv("file:///path/to/the/data.csv")

.. code:: scala

	// Run the predictions. The predictions contain all the original columns plus the predictions
	// added as new columns
	val predictions = mojo.transform(dataFrame)

	// You can easily get the predictions for desired column using the helper function as follows:
	predictions.select(mojo.selectPredictionUDF("AGE"))
