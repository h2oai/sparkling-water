|sparkling-water-logo|

|mvn-badge| |apache-2-0-license| |Powered by H2O.ai|

Sparkling Water
===============

Sparkling Water integrates `H2O-3 <https://github.com/h2oai/h2o-3/>`__, a fast scalable machine learning engine with `Apache Spark <https://spark.apache.org/>`__. It provides:

- Utilities to publish Spark data structures (RDDs, DataFrames, Datasets) as H2O-3's frames and vice versa.
- DSL to use Spark data structures as input for H2O's algorithms.
- Basic building blocks to create ML applications utilizing Spark and H2O APIs.
- Python interface enabling use of Sparkling Water directly from PySpark.

Getting Started
---------------

User Documentation
~~~~~~~~~~~~~~~~~~

`Read the documentation for Spark 3.5 <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/index.html>`__ (or
`3.4 <http://docs.h2o.ai/sparkling-water/3.4/latest-stable/doc/index.html>`__ ,
`3.3 <http://docs.h2o.ai/sparkling-water/3.3/latest-stable/doc/index.html>`__ ,
`3.2 <http://docs.h2o.ai/sparkling-water/3.2/latest-stable/doc/index.html>`__ ,
`3.1 <http://docs.h2o.ai/sparkling-water/3.1/latest-stable/doc/index.html>`__,
`3.0 <http://docs.h2o.ai/sparkling-water/3.0/latest-stable/doc/index.html>`__,
`2.4 <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/index.html>`__,
`2.3 <http://docs.h2o.ai/sparkling-water/2.3/latest-stable/doc/index.html>`__)

Download Binaries
~~~~~~~~~~~~~~~~~

`Download the latest version for Spark 3.5 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.5/latest.html>`__ (or
`3.4 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.4/latest.html>`__,
`3.3 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.3/latest.html>`__,
`3.2 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.2/latest.html>`__,
`3.1 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.1/latest.html>`__,
`3.0 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.0/latest.html>`__,
`2.4 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/latest.html>`__,
`2.3 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/latest.html>`__)

Each Sparkling Water release is also published into the Maven Central (more details below).

---------------

Try Sparkling Water!
--------------------

Sparkling Water is distributed as a Spark application library which can be used by any Spark application.
Furthermore, we provide also zip distribution which bundles the library and shell scripts.

There are several ways of using Sparkling Water:

- Sparkling Shell (Spark Shell with Sparkling Water included)
- Sparkling Water driver (Spark Submit with Sparkling Water included)
- Spark Shell and include Sparkling Water library via ``--jars`` or ``--packages`` option
- Spark Submit and include Sparkling Water library via ``--jars`` or ``--packages`` option
- PySpark with PySparkling


Run Sparkling shell
~~~~~~~~~~~~~~~~~~~

The Sparkling shell encapsulates a regular Spark shell and append Sparkling Water library on the classpath via ``--jars`` option.
The Sparkling Shell supports creation of an H2O-3 cloud and execution of H2O-3 algorithms.

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

      import ai.h2o.sparkling._
      val hc = H2OContext.getOrCreate()

   ``H2OContext`` starts H2O services on top of Spark cluster and provides primitives for transformations between H2O-3 and Spark data structures.


Use Sparkling Water with PySpark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sparkling Water can be also used directly from PySpark and the integration is called PySparkling.

See `PySparkling README <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/pysparkling.html>`__ to learn about PySparkling.

Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see how Sparkling Water can be used as Spark package, please see `Use as Spark Package <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/tutorials/use_as_spark_package.html>`__.

Use Sparkling Water in Windows environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
See `Windows Tutorial <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/tutorials/run_on_windows.html>`__ to learn how to use Sparkling Water in Windows environments.

Sparkling Water examples
~~~~~~~~~~~~~~~~~~~~~~~~
To see how to run examples for Sparkling Water, please see `Running Examples <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/devel/running_examples.html>`__.

Maven packages
~~~~~~~~~~~~~~

Each Sparkling Water release is published into Maven central with following coordinates:

- ``ai.h2o:sparkling-water-core_{{scala_version}}:{{version}}`` - Includes core of Sparkling Water
- ``ai.h2o:sparkling-water-examples_{{scala_version}}:{{version}}`` - Includes example applications
- ``ai.h2o:sparkling-water-repl_{{scala_version}}:{{version}}`` - Spark REPL integration into H2O Flow UI
- ``ai.h2o:sparkling-water-ml_{{scala_version}}:{{version}}`` - Extends Spark ML package by H2O-based transformations
- ``ai.h2o:sparkling-water-scoring_{{scala_version}}:{{version}}`` - A library containing scoring logic and definition of Sparkling Water MOJO models.
- ``ai.h2o:sparkling-water-scoring-package_{{scala_version}}:{{version}}`` - Lightweight Sparkling Water package including all dependencies required just for scoring with H2O-3 and DAI MOJO models.
- ``ai.h2o:sparkling-water-package_{{scala_version}}:{{version}}`` - Sparkling Water package containing all dependencies required for model training and scoring. This is designed to use as Spark package via ``--packages`` option.

   **Note:** The ``{{version}}`` references to a release version of Sparkling Water, the ``{{scala_version}}``
   references to Scala base version.

The full list of published packages is available
`here <http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ai.h2o%22%20AND%20a%3Asparkling-water*>`__.

--------------

Sparkling Water Backends
------------------------

Sparkling water supports two backend/deployment modes - internal and
external. Sparkling Water applications are independent on the selected
backend. The backend can be specified before creation of the
``H2OContext``.

For more details regarding the internal or external backend, please see
`Backends <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/deployment/backends.html>`__.

--------------

FAQ
---

List of all Frequently Asked Questions is available at `FAQ <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/FAQ.html>`__.

--------------

Development
-----------

Complete development documentation is available at `Development Documentation <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/devel/devel.html>`__.

Build Sparkling Water
~~~~~~~~~~~~~~~~~~~~~

To see how to build Sparkling Water, please see `Build Sparkling Water <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/devel/build.html>`__.

Develop applications with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An application using Sparkling Water is regular Spark application which
bundling Sparkling Water library. See Sparkling Water Droplet providing
an example application `here <https://github.com/h2oai/h2o-droplets/tree/master/sparkling-water-droplet>`__.

Contributing
~~~~~~~~~~~~

Just drop us a PR!
For inspiration look at our `list of issues <https://github.com/h2oai/sparkling-water/issues/new/choose>`__, feel free to create one.

Filing Bug Reports and Feature Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can file a bug report of feature request directly in Github Issues `Github Issues <https://github.com/h2oai/sparkling-water/issues/new/choose>`__.

Have Questions?
~~~~~~~~~~~~~~~

We also respond to questions tagged with sparkling-water and h2o tags on the `Stack Overflow <https://stackoverflow.com/questions/tagged/sparkling-water>`__.

Change Logs
~~~~~~~~~~~

Change logs are available at `Change Logs <http://docs.h2o.ai/sparkling-water/3.5/latest-stable/doc/CHANGELOG.html>`__.

---------------

.. |Join the chat at https://gitter.im/h2oai/sparkling-water| image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. |mvn-badge| image:: https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.12/badge.svg
   :target: http://search.maven.org/#search%7Cgav%7C1%7Cg:%22ai.h2o%22%20AND%20a:%22sparkling-water-core_2.12%22
.. |apache-2-0-license| image:: https://img.shields.io/badge/License-Apache%202-blue.svg
   :target: LICENSE
.. |sparkling-water-logo| image:: http://s3.amazonaws.com/h2o-release/h2o-classic/master/1761/docs-website/_images/sparkling-water.png
.. |Powered by H2O.ai| image:: https://img.shields.io/badge/powered%20by-h2oai-yellow.svg
   :target: https://github.com/h2oai/

