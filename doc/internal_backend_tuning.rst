Sparkling Water Tuning
----------------------

For running Sparkling Water general recommendation are:
  - Increase available memory in driver and executors (options ``spark.driver.memory`` resp., ``spark.yarn.am.memory`` and ``spark.executor.memory``).
  - Make cluster homogeneous - use the same value for driver and executor memory.
  - Increase PermGen size if you are running on top of Java7 (options ``spark.driver.extraJavaOptions`` resp., ``spark.yarn.am.extraJavaOptions`` and ``spark.executor.extraJavaOptions``).
  - In rare cases, it helps to increase ``spark.yarn.driver.memoryOverhead``, ``spark.yarn.am.memoryOverhead``, or ``spark.yarn.executor.memoryOverhead``.

For running Sparkling Water on top of Yarn:
  - Make sure that Yarn provides stable containers, do not use preemptive Yarn scheduler.
  - Make sure that Spark application manager has enough memory and increase PermGen size.
  - In case of a container failure, Yarn should not restart container and application should gracefully terminate.

Furthermore, we recommend to configure the following Spark properties to
speedup and stabilize creation of H2O services on top of Spark cluster:

+-------------------------------------------------+--------------------------+----------------------------+
| Property                                        | Value                    | Explanation                |
+=================================================+==========================+============================+
| **All environments (YARN/Standalone/Local)**    |                          |                            |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.locality.wait``                         | ``3000``                 | Number of seconds to wait  |
|                                                 |                          | for task launched on       |
|                                                 |                          | data-local node. We        |
|                                                 |                          | recommend to increase      |
|                                                 |                          | since we would like to     |
|                                                 |                          | make sure that H2O tasks   |
|                                                 |                          | are processed locally      |
|                                                 |                          | with data.                 |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.scheduler.minRegisteredResourcesRatio`` | ``1``                    | Make sure that Spark       |
|                                                 |                          | starts scheduling when it  |
|                                                 |                          | sees 100% of resources.    |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.task.maxFailures``                      | ``1``                    | Do not try to retry        |
|                                                 |                          | failed tasks.              |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.driver.extraJavaOptions``               | ``-XX:MaxPermSize=384m`` | Increase PermGem if you    |
|                                                 |                          | are running in Java7 on    |
|                                                 |                          | the Spark driver.          |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.executor.extraJavaOptions``             | ``-XX:MaxPermSize=384m`` | Increase PermGem if you    |
|                                                 |                          | are running in Java7 on    |
|                                                 |                          | the Spark executor.        |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.executor.heartbeatInterval``            | ``10s``                  | Interval between each      |
|                                                 |                          | executor heartbeats to     |
|                                                 |                          | the driver. This property  |
|                                                 |                          | should be significantly    |
|                                                 |                          | less than                  |
|                                                 |                          | ``spark.network.timeout``. |
+-------------------------------------------------+--------------------------+----------------------------+
| **YARN environment**                            |                          |                            |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.yarn.am.extraJavaOptions``              | ``-XX:MaxPermSize=384m`` | Increase PermGem if you    |
|                                                 |                          | are running in Java7 on    |
|                                                 |                          | the Yarn application       |
|                                                 |                          | master.                    |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.yarn.driver.memoryOverhead``            | increase                 | Increase memory overhead   |
|                                                 |                          | if it's necessary of the   |
|                                                 |                          | container with             |
|                                                 |                          | driver node.               |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.yarn.executor.memoryOverhead``          | increase                 | Increase memory overhead   |
|                                                 |                          | if it's necessary of the   |
|                                                 |                          | containers with            |
|                                                 |                          | executor nodes.            |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.yarn.am.memoryOverhead``                | increase                 | Increase memory overhead   |
|                                                 |                          | if it's necessary of the   |
|                                                 |                          | Yarn application master.   |
+-------------------------------------------------+--------------------------+----------------------------+
| ``spark.yarn.max.executor.failures``            | ``1``                    | Do not try to restart      |
|                                                 |                          | executors after failure    |
|                                                 |                          | and directly fail the      |
|                                                 |                          | computation.               |
+-------------------------------------------------+--------------------------+----------------------------+
