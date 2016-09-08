Docker Support
==============

Create a Docker File
--------------------

A Docker file can be created by calling ```./gradlew createDockerFile -PsparkVersion=version```, where ```version``` is spark version for which to generate docker file. The latest corresponding Sparkling Water release to be used within the Docker image is determined automatically based on the Spark version.

The gradle task can be called using ```./gradlew createDockerFile```, which creates a Docker file for the Spark version defined in gradle.properties file.

Container Requirements
----------------------

To run Sparkling Water in the container, the host has to provide a machine with at least 5G of total memory. If this is not met, Sparkling Water scripts will print a warning, but will still attempt to run.

Building a Container
--------------------

Use the following command to build a container.

::

	$ cd docker && ./build.sh


Run Bash Inside a Container
---------------------------

Use the following command to run Bash inside of a container. 

::

	$ cd docker && docker run  -i -t sparkling-water-base /bin/bash


Run Sparkling Shell Inside a Container
--------------------------------------

Use the following command to run Sparkling Shell inside of a container.

::

	$ cd docker && docker run -i -t --rm sparkling-water-base bin/sparkling-shell 

Run Examples in a Container
---------------------------

Use the following command to run examples in a container. 

::

	$ cd docker && docker run -i -t --rm sparkling-water-base bin/run-example.sh

