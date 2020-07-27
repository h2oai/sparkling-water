Import & Export H2O Frames from/to S3
-------------------------------------

Sparkling Water car read and write H2O frames from and to S3. Several configuration steps are
required.

Specify the AWS Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To enable support for S3A/S3N, we need to start Sparkling Water with the following extra dependencies:

- ``org.apache.hadoop:hadoop-aws:2.7.3``
- ``spark.jars.packages com.amazonaws:aws-java-sdk:1.7.4``

For production environments, we advise to download these jars and add them on your Spark path manually by copying them to
``$SPARK_HOME/jars`` directory.

Additionally, we can also use the ``--packages`` option when starting Sparkling Water as:

 .. code:: bash

    ./bin/sparkling-shell --packages spark.jars.packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3

The same holds for PySparkling. We can also add the following line to the spark-defaults.conf file:

 .. code:: bash

    spark.jars.packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.3

This line ensures that we don't need to specify the ``--packages`` option all the time.

Configuring S3A
~~~~~~~~~~~~~~~
In order to read and write to S3A, please add the following lines to the ``spark-defaults.conf`` file
in the ``$SPARK_HOME/conf`` directory:

 .. code:: bash

     spark.hadoop.fs.s3a.impl                org.apache.hadoop.fs.s3a.S3AFileSystem
     spark.hadoop.fs.s3a.access.key          {{AWS_ACCESS_KEY}}
     spark.hadoop.fs.s3a.secret.key          {{AWS_SECRET_KEY}}

where ``{{AWS_ACCESS_KEY}}`` should be substituted by AWS access key and ``{{AWS_SECRET_KEY}}`` by
AWS secret key.

Configuring S3N
~~~~~~~~~~~~~~~
In order to read and write to S3N, please add the following lines to the ``spark-defaults.conf`` file
in the ``$SPARK_HOME/conf`` directory:

 .. code:: bash

    spark.hadoop.fs.s3n.impl                org.apache.hadoop.fs.s3native.NativeS3FileSystem
    spark.hadoop.fs.s3n.awsAccessKeyId      {{AWS_ACCESS_KEY}}
    spark.hadoop.fs.s3n.awsSecretAccessKey  {{AWS_SECRET_KEY}}

where ``{{AWS_ACCESS_KEY}}`` should be substituted by AWS access key and ``{{AWS_SECRET_KEY}}`` by
AWS secret key.


Sparkling Water Example Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you configured Sparkling Water as explained above, please start Sparkling Shell as

 .. code:: bash

    ./bin/sparkling-shell


Next, let's start ``H2OContext``. This context brings H2O support into Spark environment:

 .. code:: scala

    import ai.h2o.sparkling._
    val hc = H2OContext.getOrCreate()

Finally, read the data:

 .. code:: scala

    import java.net.URI
    val fr = H2OFrame(new URI("s3n://data.h2o.ai/h2o-open-tour/2016-nyc/weather.csv"))

PySparkling Example Code
~~~~~~~~~~~~~~~~~~~~~~~~

When you configured PySparkling as explained above, please start PySparkling as

 .. code:: python

    ./bin/pysparkling

Next, let's start ``H2OContext``. This context brings H2O support into Spark environment:

 .. code:: python

    from pysparkling import *
    hc = H2OContext.getOrCreate()

Finally, read the data:

 .. code:: python

    fr = h2o.import_file("s3n://data.h2o.ai/h2o-open-tour/2016-nyc/weather.csv")

In PySparkling, you can also export the file to S3 as:

 .. code:: python

    h2o.export_file("s3n://path/to/target/location")


