Download H2O logs on Databricks Cloud
-------------------------------------

To download H2O logs from the Databricks cloud we need to have ``H2OContext`` already running, let's assume it's available in
``hc`` variable.

To download logs in Scala, we need to run:


.. code:: scala

    hc.downloadH2OLogs("/tmp/logs.zip")
    dbutils.fs.cp("file:/tmp/logs.zip", "/FileStore/logs.zip")


or in Python:

.. code:: python

    hc.download_h2o_logs("/tmp/logs.zip")
    dbutils.fs.cp("file:/tmp/logs.zip", "/FileStore/logs.zip")

The first method in both languages downloads the log files to Databricks filesystem. In order to make it available for
download from Databricks, we need to move the obtained logs from the Databricks filesystem to so called ``FileStore``, from
where the files can be downloaded using the web browser.

To finally download the logs to your local computer, you need to visit the following page
``https://<YOUR_DATABRICKS_INSTANCE_NAME>.cloud.databricks.com/files/logs.zip``

    Note: If you are using Community Edition of Databricks, the address is
    ``https://community.cloud.databricks.com/files/my-stuff/my-file.txt?o=######``, where the number after ``o=`` is the
    same as in your Community Edition URL.

Opening the link will download the logs to your local computer.