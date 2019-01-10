##
## Input Variables
##
variable "aws_region" {}
variable "aws_access_key" {}
variable "aws_secret_key" {}
variable "aws_subnet_id" {}
variable "aws_vpc_id" {}
variable "aws_emr_ec2_role" {
  default = "EMR_EC2_DefaultRole"
}
variable "aws_emr_role" {
  default = "EMR_DefaultRole"
}
variable "aws_core_instance_count" {}
variable "aws_instance_type" {}
variable "sw_major_version" {}
variable "sw_patch_version" {}
variable "aws_security_group_master_id" {}
variable "aws_security_group_slave_id" {}

##
## Provider Definition
##
provider "aws" {
  region = "${var.aws_region}" 
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
}

##
## Data sources
## - Roles
## - Instance Profiles
##
data "aws_iam_role" "emr_role" {
  name = "${var.aws_emr_role}"
}

data "aws_iam_role" "emr_ec2_role" {
  name = "${var.aws_emr_ec2_role}"
}

resource "aws_iam_instance_profile" "emr_ec2_instance_profile" {
  name = "EMR_EC2_instance_profile"
  role = "${data.aws_iam_role.emr_ec2_role.name}"
}

data "aws_security_group" "ElasticMapReduce-master" {
  id = "${var.aws_security_group_master_id}"
}

data "aws_security_group" "ElasticMapReduce-slave" {
  id = "${var.aws_security_group_slave_id}"
}

resource "aws_emr_cluster" "sparkling-water-cluster" {
  name          = "Sparkling-Water"
  release_label = "emr-5.17.0"
  applications  = ["Spark", "Hadoop"]

  ec2_attributes {
    subnet_id                         = "${var.aws_subnet_id}"
    emr_managed_master_security_group = "${var.aws_security_group_master_id}"
    emr_managed_slave_security_group  = "${var.aws_security_group_slave_id}"
    instance_profile                  = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
  }

  master_instance_type = "${var.aws_instance_type}"
  core_instance_type   = "${var.aws_instance_type}"
  core_instance_count  = "${var.aws_core_instance_count}"

  tags {
    name     = "SparklingWater"
  }

  bootstrap_action {
    path = "${format("s3://h2o-release/sparkling-water/rel-%s/%s/templates/aws/install_sparkling_water_%s.%s.sh",
          var.sw_major_version, var.sw_patch_version, var.sw_major_version, var.sw_patch_version)}"
    name = "Custom action"
  }


  configurations_json = <<EOF
  [
    {
      "Classification": "hadoop-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    },
    {
      "Classification": "spark-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    }
  ]
EOF

  service_role = "${data.aws_iam_role.emr_role.arn}"
}
##
## Output variables - used or created resources
##
output "emr_ec2_role" {
  value = "${data.aws_iam_role.emr_ec2_role.arn}"
}
output "emr_role" {
  value = "${data.aws_iam_role.emr_role.arn}"
}
output "emr_ec2_instance_profile" {
  value = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
}