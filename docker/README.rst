Docker Support
==============

Create DockerFile
-----------------

Docker file can be created by calling
``./gradlew createDockerFile -PsparkVersion=version``, where ``version``
is spark version for which to generate docker file. Latest corresponding
sparkling water release to be used within docker image is determined
automatically based on spark version.

The gradle task can be called without the parameter as
``./gradlew createDockerFile``, which creates docker file for spark
version defined in gradle.properties file.

Container requirements
----------------------

To run Sparkling Water in the container, the host has to provide a
machine with at least 5G of total memory. If this is not met, Sparkling
Water scripts print warning but still attempt to run.

Building a container
--------------------

.. code:: bash

    $ cd docker && ./build.sh

Run bash inside container
-------------------------

.. code:: bash

    $ cd docker && docker run  -i -t sparkling-water-base /bin/bash

Run Sparkling Shell inside container
------------------------------------

.. code:: bash

    $ cd docker && docker run -i -t --rm sparkling-water-base bin/sparkling-shell 

Running examples in container
-----------------------------

.. code:: bash

    $ cd docker && docker run -i -t --rm sparkling-water-base bin/run-example.sh
