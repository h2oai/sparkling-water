Use Sparkling Water with Amazon EMR from the Edge Node
------------------------------------------------------

Sparkling Water can be deployed on a cluster on EMR or directly on EC2 machines, which means that jobs can be submitted
from the edge node. This deployment has has several security and other benefits.

In order to start Sparkling Water from the Edge node (for example, different EC2 machine), please make sure that:

- You are able to submit Spark Jobs from the edge node first.
- You are starting Sparkling Water with ``spark.ext.h2o.client.ignore.SPARK_PUBLIC_DNS=true`` configuration.
  This configuration ensures that the H2O client will ignore ``SPARK_PUBLIC_DNS`` configuration on the client side which
  is required for the correct cloud formation. The ``SPARK_PUBLIC_DNS`` is still being applied as expected to all Spark
  services.
