Enforce System-Level Command Line Arguments in External Backend on YARN
-----------------------------------------------------------------------

System administrators can create a configuration file with implicit arguments of h2odriver and use it to make sure
the external H2O cluster is started with the specified security settings.

1. Create the config file in **/etc/h2o/sparkling-water-external.args**.
2. Specify the default command-line options that you want to enforce. Note that each argument must be on a separate line. For example:

::

    h2o_ssl_jks_internal=keystore.jks
    h2o_ssl_jks_password=password
    h2o_ssl_jts_internal=truststore.jks
    h2o_ssl_jts_password=password
