Start Sparkling Water on Amazon EMR using our Terraform Template
----------------------------------------------------------------

Sparkling Water comes with the pre-defined Terraform templates that can be used to
deploy Sparkling Water to Amazon EMR.

Before you start, you need to have Terraform installed on your machine.
If you are not familiar with Terraform, we suggest reading the `Terraform documentation <https://www.terraform.io/intro/index.html>`__.

The Terraform scripts for EMR are available in the release distribution in the
**templates/build/terraform/aws** directory or on-line in S3 as part of each Sparkling Water
release.

Sparkling Water provide 3 templates/modules:

 - ``network`` module (**/templates/build/terraform/aws/modules/network**). This module is used to set up network infrastructure on your AWS. It sets up VPC, internet gateway, subnet, routing tables and default DHCP settings.

  This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``
   - ``aws_availability_zone`` (optional) - AWS availability zone. Defaults to ``us-east-1e``.
   - ``aws_vpc_cidr_block`` (optional) - VPC CIDR block. Defaults to ``10.0.0.0/16``.
   - ``aws_subnet_cidr_block`` (optional) - VPC subnet CIDR block. Defaults to ``10.0.0.0/24``.


 - ``emr`` module  (**/templates/build/terraform/aws/modules/emr**). This module is used to start the EMR cluster on an already existing AWS network infrastructure. It sets up the correct roles, instance profiles, and security groups, and it starts EMR with the correct dependencies to run Sparkling Water.
  This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_ssh_public_key`` (mandatory) - public key (to be able to access EC2 instances via ssh later)
   - ``aws_vpc_id`` (mandatory) - ID of existing VPC
   - ``aws_subnet_id`` (mandatory) - ID of existing VPC subnet
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``.
   - ``aws_emr_version`` (optional) - EMR version. Defaults to ``emr-2.20.0``.
   - ``aws_core_instance_count`` (optional) - Number of worker nodes. Defaults to ``2``.
   - ``aws_instance_type`` (optional) - type of EC2 instances. Defaults to ``m3.xlarge``.
   - ``sw_major_version`` (optional) - Sparkling Water major version. Defaults to SUBST_SW_MAJOR_VERSION.
   - ``sw_patch_version`` (optional) - Sparkling Water minor version. Defaults to SUBST_SW_MINOR_VERSION.
   - ``jupyter_name`` (optional) - User name for Jupyter Notebook. Defaults to ``admin``.
   - ``jupyter_pass`` (optional) - Password for Jupyter Notebook. Defaults to ``admin``.

 - ``default`` module  (**/templates/build/terraform/aws**). This module is a combination of the two previous modules. It starts the network infrastructure and starts EMR with Sparkling Water on top of it.

  This module accepts the following arguments:

   - ``aws_access_key`` (mandatory) - access key to access AWS
   - ``aws_secret_key`` (mandatory) - secret key to access AWS
   - ``aws_ssh_public_key`` (mandatory) - public key (to be able to access EC2 instances via ssh later)
   - ``aws_region`` (optional) - AWS region. Defaults to ``us-east-1``.
   - ``aws_emr_version`` (optional) - EMR version. Defaults to ``emr-2.20.0``.
   - ``aws_core_instance_count`` (optional) - Number of worker nodes. Defaults to ``2``.
   - ``aws_instance_type`` (optional) - type of EC2 instances. Defaults to ``m3.xlarge``.
   - ``sw_major_version`` (optional) - Sparkling Water major version. Defaults to SUBST_SW_MAJOR_VERSION.
   - ``sw_patch_version`` (optional) - Sparkling Water minor version. Defaults to SUBST_SW_MINOR_VERSION.
   - ``jupyter_name`` (optional) - User name for Jupyter Notebook. Defaults to ``admin``.
   - ``jupyter_pass`` (optional) - Password for Jupyter Notebook. Defaults to ``admin``.


We provide these 3 templates as we realize some users already have some network infrastructure on their
AWS clusters available, and they don't want to use a template that creates it again.

You can see that one of the mandatory arguments is a public key. This public key is associated with
EC2 machines so you can actually log in via ssh. You can create yourself a key-pair using ``ssh-keygen -t rsa`` tool.

To start any template you want, please run the following in the corresponding module directory:

.. code:: bash

    terraform init
    terraform apply

You will be asked to provide mandatory variables. Please see
`Terraform documentation <https://www.terraform.io/intro/index.html>`__ for more information how to set up
variables.

To access the Jupyter Notebook, please go to https://master_public_dns:9443. The correct address is printed out as one
of the templates output. You need to approve the security exception (self-signed certificate) in your browser.

If you would like to connect to the master machine via SSH, please run:

.. code:: bash

    ssh -i path/to/private.key hadoop@public_master_dns

where ``private.key`` is the private key for the public key we specified as an input and ``public_master_dns``
is public DNS name of the master node. This dns name is printed as output after ``terraform apply`` finishes.



