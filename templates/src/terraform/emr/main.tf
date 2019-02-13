##
## Provider Definition
##
provider "aws" {
  region = "${var.aws_region}" 
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
}

resource "aws_emr_cluster" "sparkling-water-cluster" {
  name          = "Sparkling-Water"
  release_label = "${var.aws_emr_version}"
  applications  = ["Spark", "Hadoop"]

  ec2_attributes {
    subnet_id                         = "${aws_subnet.main.id}"
    emr_managed_master_security_group = "${aws_security_group.slave.id}"
    emr_managed_slave_security_group  = "${aws_security_group.master.id}"
    instance_profile                  = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
  }

  master_instance_type = "${var.aws_instance_type}"
  core_instance_type   = "${var.aws_instance_type}"
  core_instance_count  = "${var.aws_core_instance_count}"

  tags {
    name = "SparklingWater"
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

  service_role = "${aws_iam_role.emr_role.arn}"
}
