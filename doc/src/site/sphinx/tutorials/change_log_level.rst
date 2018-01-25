Change Sparkling Shell Logging Level
------------------------------------

The console output for Sparkling Water Shell by default shows verbose Spark output as well as H2O logs. If you would like to switch the output to only warnings from Spark, you need to change it in the log4j properties file in the Spark's configuration directory. To do this:

.. code:: shell

    cd $SPARK_HOME/conf
    cp log4j.properties.template log4j.properties

Then either in a text editor or vim, change the contents of the log4j.properties file from:

.. code:: shell

    #Set everything to be logged to the console
    log4j.rootCategory=INFO, console
    ...

to:

.. code:: shell

    #Set everything to be logged to the console
    log4j.rootCategory=WARN, console
    ...
