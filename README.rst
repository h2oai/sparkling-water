Sparkling Water
===============

|Documentation| |Join the chat at https://gitter.im/h2oai/sparkling-water|
|image2| |image3| |Powered by H2O.ai|

Sparkling Water integrates |H2O|'s fast scalable machine learning engine with Spark. It provides:

- Utilities to publish Spark data structures (RDDs, DataFrames, Datasets) as H2O's frames and vice versa.
- DSL to use Spark data structures as input for H2O's algorithms.
- Basic building blocks to create ML applications utilizing Spark and H2O APIs.
- Python interface enabling use of Sparkling Water directly from PySpark.

Getting Started
---------------

User Documentation
~~~~~~~~~~~~~~~~~~
The documentation contains also documentation for our clients, PySparkling and RSparkling.

- `Sparkling Water For Spark 3.0 <http://docs.h2o.ai/sparkling-water/3.0/latest-stable/doc/index.html>`__
- `Sparkling Water For Spark 2.4 <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/index.html>`__
- `Sparkling Water For Spark 2.3 <http://docs.h2o.ai/sparkling-water/2.3/latest-stable/doc/index.html>`__
- `Sparkling Water For Spark 2.2 <http://docs.h2o.ai/sparkling-water/2.2/latest-stable/doc/index.html>`__
- `Sparkling Water For Spark 2.1 <http://docs.h2o.ai/sparkling-water/2.1/latest-stable/doc/index.html>`__

Download Binaries
~~~~~~~~~~~~~~~~~

- `Latest version for Spark 3.0 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.0/latest.html>`__
- `Latest version for Spark 2.4 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.4/latest.html>`__
- `Latest version for Spark 2.3 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.3/latest.html>`__
- `Latest version for Spark 2.2 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.2/latest.html>`__
- `Latest version for Spark 2.1 <http://h2o-release.s3.amazonaws.com/sparkling-water/spark-2.1/latest.html>`__


Maven
~~~~~

Each Sparkling Water release is published into Maven central with following coordinates:

- ``ai.h2o:sparkling-water-core_{{scala_version}}:{{version}}`` - Includes core of Sparkling Water
- ``ai.h2o:sparkling-water-examples_{{scala_version}}:{{version}}`` - Includes example applications
- ``ai.h2o:sparkling-water-repl_{{scala_version}}:{{version}}`` - Spark REPL integration into H2O Flow UI
- ``ai.h2o:sparkling-water-ml_{{scala_version}}:{{version}}`` - Extends Spark ML package by H2O-based transformations
- ``ai.h2o:sparkling-water-scoring_{{scala_version}}:{{version}}`` - Lightweight package used for Scoring with H2O MOJOs. This package does not have run-time dependency on H2O cluster.
- ``ai.h2o:sparkling-water-package_{{scala_version}}:{{version}}`` - Uber Sparkling Water package containing all dependencies. This is designed to use as Spark package via ``--packages`` option

   **Note:** The ``{{version}}`` references to a release version of Sparkling Water, the ``{{scala_version}}``
   references to Scala base version.

The full list of published packages is available
`here <http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22ai.h2o%22%20AND%20a%3Asparkling-water*>`__.

Sparkling Water Requirements for Spark 3.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-  Linux/OS X/Windows
-  Java 8+
-  Python 2.7+ For Python version of Sparkling Water (PySparkling)
-  `Spark 3.0 <https://spark.apache.org/downloads.html>`__ and ``SPARK_HOME`` shell variable must point to your local Spark installation

To see requirements for older Spark version, please visit relevant documentation.

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
      val hc = H2OContext.getOrCreate()

   ``H2OContext`` starts H2O services on top of Spark cluster and provides primitives for transformations between |H2O| and Spark data structures.


Use Sparkling Water with PySpark
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sparkling Water can be also used directly from PySpark and the integration is called PySparkling.

See `PySparkling README <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/pysparkling.html>`__ to learn about PySparkling.

Use Sparkling Water via Spark Packages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To see how Sparkling Water can be used as Spark package, please see `Use as Spark Package <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/tutorials/use_as_spark_package.html>`__.

Use Sparkling Water in Windows environments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
See `Windows Tutorial <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/tutorials/run_on_windows.html>`__ to learn how to use Sparkling Water in Windows environments.

Sparkling Water examples
~~~~~~~~~~~~~~~~~~~~~~~~
To see how to run examples for Sparkling Water, please see `Running Examples <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/devel/running_examples.html>`__.

--------------

Sparkling Water Backends
------------------------

Sparkling water supports two backend/deployment modes - internal and
external. Sparkling Water applications are independent on the selected
backend. The backend can be specified before creation of the
``H2OContext``.

For more details regarding the internal or external backend, please see
`Backends <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/deployment/backends.html>`__.

--------------

FAQ
---

List of all Frequently Asked Questions is available at `FAQ <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/FAQ.html>`__.

--------------

Development
-----------

Complete development documentation is available at `Development Documentation <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/devel/devel.html>`__.

Build Sparkling Water
~~~~~~~~~~~~~~~~~~~~~

To see how to build Sparkling Water, please see `Build Sparkling Water <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/devel/build.html>`__.

Develop applications with Sparkling Water
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

An application using Sparkling Water is regular Spark application which
bundling Sparkling Water library. See Sparkling Water Droplet providing
an example application `here <https://github.com/h2oai/h2o-droplets/tree/master/sparkling-water-droplet>`__.

Contributing
~~~~~~~~~~~~

Look at our `list of JIRA
tasks <https://0xdata.atlassian.net/projects/SW/issues>`__ or send your idea to support@h2o.ai.

Filing Bug Reports and Feature Requests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can file a bug report of feature request directly in the Sparkling Water JIRA page at `http://jira.h2o.ai/ <https://0xdata.atlassian.net/projects/SW/issues>`__.

1. Log in to the Sparkling Water JIRA tracking system. (Create an account if necessary.)

2. Once inside the home page, click the **Create** button.

   .. figure:: /doc/src/site/sphinx/images/jira_create.png
      :alt: center

3. A form will display allowing you to enter information about the bug or feature request.

   .. figure:: /doc/src/site/sphinx/images/jira_new_issue.png
      :alt: center

   Enter the following on the form:

   - Select the Project that you want to file the issue under. For example, if this is an open source public bug, you should file it under **SW (SW)**.
   - Specify the Issue Type. For example, if you believe you've found a bug, then select **Bug**, or if you want to request a new feature, then select **New Feature**.
   - Provide a short but concise summary about the issue. The summary will be shown when engineers organize, filter, and search for Jira tickets.
   - Specify the urgency of the issue using the Priority dropdown menu.
   - If there is a due date specify it with the Due Date.
   - The Components drop down refers to the API or language that the issue relates to. (See the drop down menu for available options.)
   - You can leave Affects Version/s, Fix Version\s, and Assignee fields blank. Our engineering team will fill this in.
   - Add a detailed description of your bug in the Description section. Best practice for descriptions include:

   - A summary of what the issue is
   - What you think is causing the issue
   - Reproducible code that can be run end to end without requiring an engineer to edit your code. Use {code} {code} around your code to make it appear in code format.
   - Any scripts or necessary documents. Add by dragging and dropping your files into the create issue dialogue box.

   You can be able to leave the rest of the ticket blank.

4. When you are done with your ticket, simply click on the **Create** button at the bottom of the page.

   .. figure:: /doc/src/site/sphinx/images/jira_finished_create.png
      :alt: center

After you click **Create**, a pop up will appear on the right side of your screen with a link to your Jira ticket. It will have the form `https://0xdata.atlassian.net/browse/SW-####`. You can use this link to later edit your ticket.

Please note that your Jira ticket number along with its summary will appear in one of the Jira ticket slack channels, and anytime you update the ticket anyone associated with that ticket, whether as the assignee or a watcher will receive an email with your changes.

Have Questions?
~~~~~~~~~~~~~~~

We also respond to questions tagged with sparkling-water and h2o tags on the `Stack Overflow <https://stackoverflow.com/questions/tagged/sparkling-water>`__.

Change Logs
~~~~~~~~~~~

Change logs are available at `Change Logs <http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/CHANGELOG.html>`__.

---------------

.. |Documentation| image:: https://media.readthedocs.org/static/projects/badges/passing.svg
   :target: http://docs.h2o.ai/sparkling-water/2.4/latest-stable/doc/index.html
.. |Join the chat at https://gitter.im/h2oai/sparkling-water| image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/h2oai/sparkling-water?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. |image2| image:: https://maven-badges.herokuapp.com/maven-central/ai.h2o/sparkling-water-core_2.11/badge.svg
   :target: http://search.maven.org/#search%7Cgav%7C1%7Cg:%22ai.h2o%22%20AND%20a:%22sparkling-water-core_2.11%22
.. |image3| image:: https://img.shields.io/badge/License-Apache%202-blue.svg
   :target: LICENSE
.. |Powered by H2O.ai| image:: https://img.shields.io/badge/powered%20by-h2oai-yellow.svg
   :target: https://github.com/h2oai/
.. |H2O| replace:: H\ :sub:`2`\ O

