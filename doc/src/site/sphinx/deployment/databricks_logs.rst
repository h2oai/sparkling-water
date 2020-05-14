Download H2O Logs from Databricks Cloud
---------------------------------------

To download H2O logs from the Databricks cloud, we need to have ``H2OContext`` already running.
Let's assume it's available in the ``hc`` variable.

To download logs, please run:

.. content-tabs::

    .. tab-container:: Scala
        :title: Scala

        .. code:: scala

            val path = hc.downloadH2OLogs("/tmp")
            dbutils.fs.cp("file://" + path, "/FileStore/logs.zip")

    .. tab-container:: Python
        :title: Python

        .. code:: python

            path = hc.downloadH2OLogs("/tmp")
            dbutils.fs.cp("file://" + path, "/FileStore/logs.zip")

The first method downloads the log files to the Databricks filesystem. In order to make it available for
download from Databricks, we need to move the obtained logs from the Databricks filesystem to the ``FileStore``, which
is where the files can be downloaded using a web browser.

Finally, to download the logs to your local computer, you need to visit the following page
``https://<YOUR_DATABRICKS_INSTANCE_NAME>.cloud.databricks.com/files/logs.zip``.

Opening that link will download the logs to your local computer.

**Note**: If you are using Community Edition of Databricks, the address is
``https://community.cloud.databricks.com/files/logs.zip?o=######``, where the number after ``o=`` is the same
as in your Community Edition URL.
