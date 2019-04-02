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
  copy MySQL metastore jdbc connector. This is not required in YARN cluster mode.



This is all preparation we need to do. The following code shows how to import the table.

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        To read data from Hive in Sparkling Water, you can use the method:

        .. code:: Scala

            val airlinesTable = h2oContext.importHiveTable("default", "airlines")

    .. tab-container:: Python
        :title: Python

        To read data from Hive in PySparkling, you can use the method:

        .. code:: python

            airlines_frame = h2o.import_hive_table("default", "airlines")

    .. tab-container:: R
        :title: R

        To read data from Hive in RSparkling, you can use the method:

        .. code:: python

            airlines_frame = h2o.import_hive_table("default", "airlines")

This call reads airlines table from default database.

