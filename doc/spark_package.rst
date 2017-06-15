Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water is also published as a Spark package. You can use it
directly from your Spark distribution.

For example, if you have Spark version 2.0 and would like to use
Sparkling Water version 2.0.0 and launch example
``CraigslistJobTitlesStreamingApp``, then you can use the following
command:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.11:2.1.0,ai.h2o:sparkling-water-examples_2.11:2.1.0 --class org.apache.spark.examples.h2o.CraigslistJobTitlesStreamingApp /dev/null

The Spark option ``--packages`` points to published Sparkling Water
packages in Maven repository.

The similar command works for ``spark-shell``:

.. code:: bash

    $SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-core_2.11:2.1.0,ai.h2o:sparkling-water-examples_2.11:2.1.0

The same command works for Python programs:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-core_2.11:2.1.0,ai.h2o:sparkling-water-examples_2.11:2.1.0 example.py


Note: When you are using Spark packages you do not need to download Sparkling Water distribution! Spark installation is sufficient!





