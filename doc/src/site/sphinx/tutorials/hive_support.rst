Hive Support in Sparkling Water
-------------------------------------

Spark support reading data natively from Hive and H2O as well. In Sparkling Water you can decide which
tool you want to use for this task. This tutorial explains what is need to use H2O to read data from Hive in
Sparkling Water environment.

Preparation
~~~~~~~~~~~

- Make sure ``$SPARK_HOME/conf`` contains the hive-site.xml with your Hive configuration.
- In YARN client mode or any local mode, please copy the required connector jars for your Metastore to ``$SPARK_HOME/jars``.
  You can find these jars in ``$HIVE_HOME/lib directory``. For example, if you are using MySQL as a Metastore for Hive,
  copy metastore jdbc connector. This is not required in YARN cluster mode.



This is all preparation we need to do.

Running
~~~~~~~

Currently, only PySparkling and RSparkling is supported. Scala API is yet to be implemented.

To read data from HIVE in PySparkling, you can use the method

.. code:: python

    airlines_frame = h2o.import_hive_table("default", "airlines")

This call will read airlines table from default database. Similar method exists in RSparkling.

