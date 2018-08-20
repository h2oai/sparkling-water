Use Sparkling Water with Amazon EMR from the Edge Node
------------------------------------------------------

Sparkling Water can be deployed on a cluster on EMR or directly on EC2 machines. The jobs can be submitted
from the edge node which has several security and other benefits.

In order to start Sparkling Water from the Edge node (for example, different EC2 machine), please make sure that:

- You are able to submit Spark Jobs from the edge node first
- You are starting Sparkling Water with ``spark.ext.h2o.client.ignore.SPARK_PUBLIC_DNS=true`` configuration.
  This configuration ensures that the H2O client will ignore ``SPARK_PUBLIC_DNS`` configuration on the client side which
  is required for the correct cloud formation. The ``SPARK_PUBLIC_DNS`` is still being applied as expected to all Spark
  services.
- Make sure that ``DNS resolution`` and ``DNS hostnames`` are set to ``true`` in the AWS DNS configuration. If you can't
  set these option to ``true``, AWS provided DNS server
  will not be able to resolve internal DNS names. In that case, please start Sparkling Water with additional option
  ``spark.ext.h2o.ip.based.flatfile=true``, which ensures that all Sparkling Water worker nodes prefer to use IP addresses
  instead of unresolvable hostnames.

