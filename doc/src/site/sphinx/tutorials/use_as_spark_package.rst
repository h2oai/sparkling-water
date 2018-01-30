Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Sparkling Water is also published as a Spark package. You can use it directly from your Spark distribution. The name of the published package is **ai.h2o:sparkling-water-package**, and it references all published Sparkling Water modules. Moreover, each module can be used as a Spark package if necessary.

Id you would like to use Sparkling Water version SUBST_SW_VERSION on Spark 2.2 and launch example
``CraigslistJobTitlesStreamingApp``, then you can use the following
command:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-package_2.11:SUBST_SW_VERSION --class org.apache.spark.examples.h2o.CraigslistJobTitlesStreamingApp /dev/null

The Spark option ``--packages`` points to the Duke package and the published Sparkling Water packages in the Maven repository.

A similar command works for ``spark-shell``:

.. code:: bash

    $SPARK_HOME/bin/spark-shell --packages ai.h2o:sparkling-water-package_2.11:SUBST_SW_VERSION

The same command works for Python programs:

.. code:: bash

    $SPARK_HOME/bin/spark-submit --packages ai.h2o:sparkling-water-package_2.11:SUBST_SW_VERSION example.py


**Note**: When you are using Spark packages, you do not need to download the Sparkling Water distribution. The Spark installation is sufficient.

