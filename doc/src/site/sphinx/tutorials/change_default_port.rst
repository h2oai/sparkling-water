.. _change_default_port:

Changing the Default Port
-------------------------

By default, H2O on Spark starts on the IP of the client node and the first free port starting at 54321. You can specify to use a custom port for H2O using the ``spark.ext.h2o.client.web.port``, as in below. Refer to :ref:`sw_config_properties` for a complete list of configuration properties.

.. code:: bash

   ./bin/sparkling-shell --conf "spark.ext.h2o.client.web.port=443"
