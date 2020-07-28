Memory Allocation
-----------------

H2O resides in the same executor JVM as Spark. The memory provided for H2O is configured via Spark. Refer to `Spark Configuration <http://spark.apache.org/docs/latest/configuration.html>`__ for more details.

Generic Configuration
~~~~~~~~~~~~~~~~~~~~~

-  Configure the Executor memory (i.e., memory available for H2O) via the Spark configuration property ``spark.executor.memory``.

   For example, ``bin/sparkling-shell --conf spark.executor.memory=5g``, or configure the property in ``$SPARK_HOME/conf/spark-defaults.conf``

-  Configure the Driver memory (i.e., memory available for H2O client running inside Spark driver) via the Spark configuration property ``spark.driver.memory``

   For example, ``bin/sparkling-shell --conf spark.driver.memory=4g``, or configure the property in ``$SPARK_HOME/conf/spark-defaults.conf``

YARN-Specific Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Refer to the `Spark Configuration documentation <http://spark.apache.org/docs/latest/configuration.html>`__.

-  For JVMs that require a large amount of memory, we strongly recommend configuring the maximum amount of memory available for individual mappers. For information on how to do this using YARN, refer to http://docs.h2o.ai/h2o/latest-stable/h2o-docs/faq/hadoop.html
