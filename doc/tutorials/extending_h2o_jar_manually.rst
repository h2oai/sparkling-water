Extending H2O jar manually
--------------------------

It is also possible to extend the H2O jar for the Sparkling Water external backend
with required classes manually. This may be useful in cases when you want to
try external Sparkling Water backend with custom H2O and Sparkling Water versions
which have not been released yet.

Before you start, please clone and build Sparkling Water. Sparkling
Water can be built using Gradle as ``./gradlew build -x check``.

In order to extend H2O/H2O driver jar file, Sparkling Water build
process has Gradle task ``extendJar`` which can be configured in various
ways.

The recommended way for the user is to call
``./gradlew extendJar -PdownloadH2O``. The ``downloadH2O`` Gradle
command line argument tels the task to download correct h2o version for
current sparkling water from our repository automatically. The
downloaded jar is cached for future calls. If ``downloadH2O`` is run
without any argument, then basic H2O jar is downloaded, but
``downloadH2O`` command takes also argument specifying hadoop version,
such as cdh5.8 or hdp2.6. In case the argument is specified, for example
as ``downloadH2O=cdh5.8``, instead of downloading H2O jar, H2O driver
jar for specified hadoop version will be downloaded.

The location of H2O/H2O driver jar to be extended can also be set
manually via ``H2O_ORIGINAL_JAR`` environmental variable. No work is
done if the file is not available on this location or if it's not
correct H2O or H2O driver jar.

Gradle property has higher priority if both ``H2O_ORIGINAL_JAR`` and
``-PdownloadH2O`` are set

Here is a few few examples how H2O/H2O driver jar can be extended:

1) In this case the jar to be extended is located using the provided
   environment variable.

.. code:: bash

    export H2O_ORIGINAL_JAR = ...
    ./gradlew extendJar

2) In this case the jar to be extended is H2O jar and the correct
   version is downloaded from our repository first.

.. code:: bash

    ./gradlew extendJar -PdownloadH2O

3) In this case the jar to be extended is H2O driver jar for provided
   hadoop version and is downloaded from our repository first

.. code:: bash

    ./gradlew extendJar -PdownloadH2O=cdh5.4

4) This case will throw an exception since such hadoop version is not
   supported.

.. code:: bash

    ./gradlew extendJar -PdownloadH2O=abc

5) This version will ignore environment variable and jar to be extended
   will be downloaded from our repository. The same holds for version
   with the argument.

.. code:: bash

    export H2O_ORIGINAL_JAR = ...
    ./gradlew extendJar -PdownloadH2O

The ``extendJar`` tasks also prints a path to the extended jar. It can
be saved to environmental variable as

.. code:: bash

    export H2O_EXTENDED_JAR=`./gradlew -q extendJar -PdownloadH2O`

Now we have the the jar file for either regular H2O or H2O driver ready!
