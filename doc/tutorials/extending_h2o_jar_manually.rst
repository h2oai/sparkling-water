.. _extend_jar_manually:

Extending H2O Jar Manually
--------------------------

It is also possible to extend the H2O jar for the Sparkling Water external backend with required classes manually. This may be useful in cases when you want to try the external Sparkling Water backend with custom H2O and Sparkling Water versions that have not been released yet.

Before you start, please clone and build Sparkling Water. Sparkling Water can be built using Gradle as ``./gradlew build -x check``.

In order to the extend H2O/H2O driver jar file, the Sparkling Water build process has a Gradle task ``extendJar``, which can be configured in various ways.

The recommended way to configure is for the user is to call ``./gradlew -PdoExtend extendJar -PdownloadH2O``. The ``downloadH2O`` Gradle command line argument tells the task to download the correct H2O  version for the current Sparkling Water from our repository automatically. The downloaded jar is cached for future calls. If ``downloadH2O`` is run without any argument, then the basic H2O jar is downloaded. But the ``downloadH2O`` command also accepts an argument specifying the Hadoop version, such as cdh5.8 or hdp2.6. In case the argument is specified, for example as ``downloadH2O=cdh5.8``, instead of downloading the H2O jar, the H2O driver jar for the specified Hadoop version will be downloaded.

The location of the H2O/H2O driver jar that is to be extended can also be set manually via the ``H2O_ORIGINAL_JAR`` environmental variable. No work is done if the file is not available on this location or if it's not the correct H2O or H2O driver jar.

The gradle property has a higher priority if both ``H2O_ORIGINAL_JAR`` and ``-PdownloadH2O`` are set.

Here is a few few examples how H2O/H2O driver jar can be extended:

- In this case, the jar to be extended is located using the provided environment variable.

 .. code:: bash

    export H2O_ORIGINAL_JAR = ...
    ./gradlew -PdoExtend extendJar

- In this case, the jar to be extended is the H2O jar, and the correct version is downloaded from our repository first.

 .. code:: bash

    ./gradlew -PdoExtend extendJar -PdownloadH2O

- In this case, the jar to be extended is the H2O driver jar for the provided Hadoop version and is downloaded from our repository first.

 .. code:: bash

    ./gradlew -PdoExtend extendJar -PdownloadH2O=cdh5.4

- This case will throw an exception because the specified Hadoop version is not supported.

 .. code:: bash

    ./gradlew -PdoExtend extendJar -PdownloadH2O=abc

- This example will ignore the environment variable, and the jar to be extended will be downloaded from our repository. 

 .. code:: bash

    export H2O_ORIGINAL_JAR = ...
    ./gradlew -PdoExtend extendJar -PdownloadH2O

The ``extendJar`` task also prints a path to the extended jar. It can be saved to the environment variable as:

 .. code:: bash

    export H2O_EXTENDED_JAR=`./gradlew -q -PdoExtend extendJar -PdownloadH2O`

Now we have the the jar file for either regular H2O or the H2O driver ready!
