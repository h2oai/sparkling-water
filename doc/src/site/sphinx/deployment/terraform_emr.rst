Start Sparkling Water on Amazon EMR using our Terraform Template
----------------------------------------------------------------

Sparkling Water comes with the pre-defined Terraform templates which can be used to
deploy Sparkling Water to Amazon EMR.

Before we start, we need to have Terraform installed on our machine.
If you are not familiar with Terraform, we suggest reading `Terraform documentation <https://www.terraform.io/intro/index.html>`__.

The Terraform scripts for EMR are available in the release distribution at
``templates/build/terraform/aws`` directory or on-line in S3 as part of each Sparkling Water
release.

We provide 3 templates:

 - ``network`` module (``/templates/build/terraform/aws/modules/network``). This module is used to set up network infrastructure on your AWS.
   It sets up VPC, internet gateway, subnet, routing tables and default DHCP settings.
   This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``
   - ``aws_availability_zone`` (optional) - AWS availability zone. Defaults to ``us-east-1e``
   - ``aws_vpc_cidr_block`` (optional) - VPC CIDR block. Defaults to ``10.0.0.0/16``
   - ``aws_subnet_cidr_block`` (optional) - VPC subnet CIDR block. Defaults to ``10.0.0.0/24``

 - ``emr`` module  (``/templates/build/terraform/aws/modules/emr``). This module is used to start EMR cluster on already existing AWS network infrastructure.
  It sets up right roles, instance profiles, security groups and starts EMR with correct dependencies to run Sparkling
  Water.
  This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_vpc_id`` (mandatory) - ID of existing VPC
   - ``aws_subnet_id`` (mandatory) - ID of existing VPC subnet
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``
   - ``aws_emr_version`` (optional) - EMR version. Defaults to ``emr-2.20.0``
   - ``aws_core_instance_count`` (optional) - Number of worker nodes. Defaults to ``2``
   - ``aws_instance_type`` (optional) - type of EC2 instances. Defaults to ``m3.xlarge``
   - ``sw_major_version`` (optional) - Sparkling Water major version. Defaults to SUBST_SW_MAJOR_VERSION
   - ``sw_patch_version`` (optional) - Sparkling Water minor version. Defaults to SUBST_SW_MINOR_VERSION


 - ``default`` module  (``/templates/build/terraform/aws``). This module is combination of the two previous modules. It starts the network
  infrastructure and starts EMR with Sparkling Water on top of it.
  This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``
   - ``aws_emr_version`` (optional) - EMR version. Defaults to ``emr-2.20.0``
   - ``aws_core_instance_count`` (optional) - Number of worker nodes. Defaults to ``2``
   - ``aws_instance_type`` (optional) - type of EC2 instances. Defaults to ``m3.xlarge``
   - ``sw_major_version`` (optional) - Sparkling Water major version. Defaults to SUBST_SW_MAJOR_VERSION
   - ``sw_patch_version`` (optional) - Sparkling Water minor version. Defaults to SUBST_SW_MINOR_VERSION


We provide these 3 templates as we realize some users have already some network infrastructure on their
AWS clusters available and they don't want to use template which create it again.

To start any template you want, please run

.. code:: bash

    terraform init
    terraform apply

in the corresponding template directory. You will be asked for mandatory variables. Please see
`Terraform documentation <https://www.terraform.io/intro/index.html>`__ for more information how to set up
variables.
