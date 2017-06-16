Sparkling Water
===============

|Join the chat at https://gitter.im/h2oai/sparkling-water| |image1|
|image2| |image3| |Powered by H2O.ai|

Sparkling Water integrates |H2O|'s fast scalable machine learning engine with Spark. It provides:

- Utilities to publish Spark data structures (RDDs, DataFrames, Datasets) as H2O's frames and vice versa.
- DSL to use Spark data structures as input for H2O's algorithms.
- Basic building blocks to create ML applications utilizing Spark and H2O APIs.
- Python interface enabling use of Sparkling Water directly from PySpark.

Getting Started
---------------

Select right version
~~~~~~~~~~~~~~~~~~~~

The Sparkling Water is developed in multiple parallel branches. Each
branch corresponds to a Spark major release (e.g., branch **rel-2.1**
provides implementation of Sparkling Water for Spark **2.1**).

Please, switch to the right branch:

- For Spark 2.1 use branch `rel-2.1 <https://github.com/h2oai/sparkling-water/tree/rel-2.1>`__
- For Spark 2.0 use branch `rel-2.0 <https://github.com/h2oai/sparkling-water/tree/rel-2.0>`__
- For Spark 1.6 use branch `rel-1.6 <https://github.com/h2oai/sparkling-water/tree/rel-1.6>`__ (Only critical fixes)

   **Note:** The `master <https://github.com/h2oai/sparkling-water/tree/master>`__
   branch includes the latest changes for the latest Spark version.
   They are back-ported into older Sparkling Water versions.

Requirements
~~~~~~~~~~~~

-  Linux/OS X/Windows
-  Java 7+
-  Python 2.6+ For Python version of Sparkling Water (PySparkling)
-  `Spark 1.6+ <https://spark.apache.org/downloads.html>`__ and ``SPARK_HOME`` shell variable must point to your local Spark installation


Download Binaries
~~~~~~~~~~~~~~~~~

For each Sparkling Water you can download binaries here:

- `Sparkling Water - Latest version <http://h2o-release.s3.amazonaws.com/sparkling-water/master/latest.html>`__
- `Sparkling Water - Latest 2.1 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.1/latest.html>`__
- `Sparkling Water - Latest 2.0 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-2.0/latest.html>`__
- `Sparkling Water - Latest 1.6 version <http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.6/latest.html>`__

Maven
~~~~~

Each Sparkling Water release is published into Maven central. Published artifacts are provided with the following Scala
versions:

- Sparkling Water 2.1.x - Scala 2.11
- Sparkling Water 2.0.x - Scala 2.11
- Sparkling Water 1.6.x - Scala 2.10

The artifacts coordinates are:

- ``ai.h2o:sparkling-water-core_{{scala_version}}:{{version}}`` - includes core of Sparkling Water.
- ``ai.h2o:sparkling-water-examples_{{scala_version}}:{{version}}`` - includes example applications.

   **Note:** The ``{{version}}`` references to a release version of Sparkling Water, the ``{{scala_version}}``
   references to Scala base version (``2.10`` or ``2.11``). For example:
   ``ai.h2o:sparkling-water-examples_2.11:2.1.0``

The full list of published packages is available
`here <http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ai.h2o%22%20AND%20a%3Asparkling-water*>`__.

---------------

Use Sparkling Water
-------------------

Sparkling Water is distributed as a Spark application library which can be used by any Spark application.
Furthermore, we provide also zip distribution which bundles the library and shell scripts.

There are several ways of using Sparkling Water:

- Sparkling Shell
- Sparkling Water driver
- Spark Shell and include Sparkling Water library via ``--jars`` or ``--packages`` option
- Spark Submit and include Sparkling Water library via ``--jars`` or ``--packages`` option
- PySpark with PySparkling


Run Sparkling shell
~~~~~~~~~~~~~~~~~~~

The Sparkling shell encapsulates a regular Spark shell and append Sparkling Water library on the classpath via ``--jars`` option.
The Sparkling Shell supports creation of an |H2O| cloud and execution of |H2O| algorithms.

1. Either download or build Sparkling Water
2. Configure the location of Spark cluster:

   .. code:: bash

      export SPARK_HOME="/path/to/spark/installation"
      export MASTER="local[*]"


   In this case, ``local[*]`` points to an embedded single node cluster.

3. Run Sparkling Shell:

   .. code:: bash

      bin/sparkling-shell

   Sparkling Shell accepts common Spark Shell arguments. For example, to increase memory allocated by each executor, use the ``spark.executor.memory`` parameter: ``bin/sparkling-shell --conf "spark.executor.memory=4g"``

4. Initialize H2OContext

   .. code:: scala

      import org.apache.spark.h2o._
      val hc = H2OContext.getOrCreate(spark)

   ``H2OContext`` starts H2O services on top of Spark cluster and provides primitives for transformations between |H2O| and Spark data structures.


Use Sparkling Water with PySpark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sparkling Water can be also used directly from PySpark and the integration is called PySparkling.

See `PySparkling README <py/README.rst>`__ to learn about PySparkling.

Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see how Sparkling Water can be used as Spark package, please see `Use as Spark Package <doc/spark_package.rst>`__.

Use Sparkling Water in Windows environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
See `Windows Tutorial <doc/windows_manual.rst>`__ to learn how to use Sparkling Water in Windows environments.

Sparkling Water examples
~~~~~~~~~~~~~~~~~~~~~~~~
To see how to run examples for Sparkling Water, please see `Running Examples <doc/running_examples.rst>`__.

--------------

Sparkling Water Backends
------------------------

Sparkling water supports two backend/deployment modes - internal and
external. Sparkling Water applications are independent on the selected
backend. The backend can be specified before creationg of the
``H2OContext``.

For more details regarding the internal or external backend, please see
`Backends <doc/backends.rst>`__.

--------------

FAQ
---

List of all Frequently Asked Questions is available at `FAQ <doc/FAQ.rst>`__.

--------------

Development
-----------

Complete development documentation is available at `Development Documentation <DEVEL.md>`__.

Build Sparkling Water
~~~~~~~~~~~~~~~~~~~~~

To see how to build Sparkling Water, please see `Build Sparkling Water <doc/build.rst>`__.

Develop applications with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An application using Sparkling Water is regular Spark application which
bundling Sparkling Water library. See Sparkling Water Droplet providing
an example application `here <https://github.com/h2oai/h2o-droplets/tree/master/sparkling-water-droplet>`__.

Contributing
~~~~~~~~~~~~

Look at our `list of JIRA
tasks <https://0xdata.atlassian.net/issues/?filter=13600>`__ for new
contributors or send your idea to support@h2o.ai.

Issues
~~~~~~

To report issues, please use our JIRA page at
`http://jira.h2o.ai/ <https://0xdata.atlassian.net/projects/SW/issues>`__.

We also respond to questions tagged with sparkling-water and h2o tags on
the `Stack
Overflow <https://stackoverflow.com/questions/tagged/sparkling-water>`__.

--------------

.. |Join the chat at https://gitter.im/h2oai/sparkling-water| image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. |image1| image:: https://travis-ci.org/h2oai/sparkling-water.svg?branch=master
   :target: https://travis-ci.org/h2oai/sparkling-water
.. |image2| image:: https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.11/badge.svg
   :target: http://search.maven.org/#search%7Cgav%7C1%7Cg:%22ai.h2o%22%20AND%20a:%22sparkling-water-core_2.11%22
.. |image3| image:: https://img.shields.io/badge/License-Apache%202-blue.svg
   :target: LICENSE
.. |Powered by H2O.ai| image:: https://img.shields.io/badge/powered%20by-h2oai-yellow.svg
   :target: https://github.com/h2oai/
.. |H2O| replace:: H\ :sub:`2`\ O
