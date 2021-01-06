Use Sparkling Water with Amazon EMR
-----------------------------------

This section describes how to use Sparkling Water with Amazon EMR via the Web Services UI. An example describing how to do this using the AWS CLI is available in the `Analytics at Scale: H2O, Apache Spark and R on AWS EMR <https://redoakstrategic.com/h2oaws/>`__ blog post (courtesy of Red Oak Strategic). 

To use Sparkling Water with an EMR cluster, you can use a premade H2O template.

1. Log in to the `AWS Marketplace <https://aws.amazon.com/marketplace/>`__. 

2. In the AMI & SaaS search bar, search for H2O, and select **H2O Artificial Intelligence** to open H2O in the marketplace. Review the information on this page. Note that the Delivery Methods section provides two options:

   - **H2O Cluster of VM**: All the VPC, subnets, and network security groups are created for you.
   - **H2O Cluster of VM - Bring your own VPC**: All the networks security groups, subnets, and internet gateways are created by the user.
   
   Click **Continue** after reviewing to open the Launch on the EC2 page.

  .. figure:: ../images/aws_h2oai.png
      :alt: H2O Artificial Intelligence 

3. The Launch on the EC2 page provides information about launch options. On the Manual Launch tab:

   - Select the offering that you prefer.
   - Select the region to launch.
   - Specify a Deployment option.

   Click **Launch with CloudFormation Console** to begin creating your stack.

  .. figure:: ../images/aws_launch_on_ec2.png
     :alt: Launch On EC2 page

4. On the Select Template page, enter https://s3-us-west-2.amazonaws.com/h2o-cloud/aws-template/TemplateEMR.json in the **Specify an Amazon S3 template URL** field.

 This template requires an install script, which is available here: https://s3.amazonaws.com/h2o-release/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_SW_VERSION/templates/aws/install_sparkling_water_SUBST_SW_VERSION.sh. This script can be added using one of the following methods:

  - Download and place the script in the following s3 path: s3://h2o-cloud/aws-template/install_sparkling_water_SUBST_SW_VERSION.sh
  
   or
  
  - Modify the s3 path to the script in the TemplateEMR.json file

  Click **Next** when you are done.

  .. figure:: ../images/aws_select_template_emr.png
     :alt: Select template - EMR


5. Note that the Specify Details page now includes an EMR Options section. Enter a name for the stack, update any options as desired, and then click **Next** to continue.

  .. figure:: ../images/aws_specify_details_emr.png
     :alt: Specify details - EMR

6. Enter any optional tags and/or permissions on the Options page. Click **Next** to continue.

  .. figure:: ../images/aws_options.png
     :alt: Options page

7. Review the Stack configuration. Click **Create** to create the Stack or click **Previous** to return to another page and edit any information.

After your EMR cluster is created, you can ssh into your head node. In the head node, you will find an H2O folder with Sparkling Water inside. To start any H2O jobs with Sparkling Water, follow the instructions located on the download page (https://www.h2o.ai/download/).

**Note**: It is important to add the following Spark configuration to any of your EMR jobs:

::

  --conf spark.dynamicAllocation.enabled=false  --conf spark.scheduler.minRegisteredResourcesRatio=1

and for EMR 5.x (``5.32.0`` and newer) also:

::

  --conf spark.yarn.heterogeneousExecutors.enabled=false

