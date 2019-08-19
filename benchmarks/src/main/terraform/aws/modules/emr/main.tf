##
## Provider Definition
##
provider "aws" {
  region = "${var.aws_region}"
  access_key = "${var.aws_access_key}"
  secret_key = "${var.aws_secret_key}"
}

data "aws_vpc" "main" {
  id = "${var.aws_vpc_id}"
}

data "aws_subnet" "main" {
  id = "${var.aws_subnet_id}"
}

resource "aws_key_pair" "key" {
  public_key = "ssh-rsa ${var.aws_ssh_public_key == "" ? "AAAAB3NzaC1yc2EAAAADAQABAAABAQC0eX0fhy3WTIHF13DuSTHBFjLzKRssFRrW6e2B+/9Oh2Ua/zsEoIeLyX5YtPAqeR22DVJBA+sOvKMQnenAVUa0XG7y6rzEPgugqWNv6NVsFgbgHMfWpRYcuPuOo42T0AQD/9rLViyAzy6lRDid3gpN3PkSBhDLGPEZYs9Lzucawm2FZV92/9u5CxgvRZBAAIrWtgHwGpos3mVuisNxHjH3uEv0B43NzN5hJfBYiEyHhwi2eyjTuDFvVQ8rywcrDZ+aR2BTRX+roR7eVq7isjyOq41qy+pRsRLl8/9ULA6HvDYyozN+jCd5xhFJHTMG1IInapIUcRewtqzsgA9XggyT" : var.aws_ssh_public_key}"
}

resource "aws_s3_bucket" "sw_bucket" {
  acl = "private"
  force_destroy = true
  tags = {
    Name= "SparklingWaterBenchmarksDeploymentBucket"
  }
}

resource "aws_s3_bucket_object" "benchmarks_jar" {
  bucket = "${aws_s3_bucket.sw_bucket.bucket}"
  key = "benchmarks.jar"
  source = "${var.sw_package_file}"
}

resource "aws_emr_cluster" "sparkling-water-cluster" {
  name = "Sparkling-Water-Benchmarks"
  release_label = "${var.aws_emr_version}"
  log_uri = "s3://${aws_s3_bucket.sw_bucket.bucket}/"
  applications = ["Spark", "Hadoop"]

  ec2_attributes {
    subnet_id = "${data.aws_subnet.main.id}"
    key_name = "${aws_key_pair.key.key_name}"
    emr_managed_master_security_group = "${aws_security_group.slave.id}"
    emr_managed_slave_security_group = "${aws_security_group.master.id}"
    instance_profile = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
  }

  master_instance_group {
    instance_type = "${var.aws_instance_type}"
  }

  core_instance_group {
    instance_type = "${var.aws_instance_type}"
    instance_count = "${var.aws_core_instance_count}"
  }

  tags = {
    name = "SparklingWaterBenchmarks"
  }

  step {
    action_on_failure = "TERMINATE_CLUSTER"
    name   = "ExecuteBenchmarks"

    hadoop_jar_step {
      jar  = "command-runner.jar"
      args = [
        "spark-submit",
        "--class", "ai.h2o.sparkling.benchmarks.Runner",
        "--master", "yarn",
        "--deploy-mode", "client",
        "--executor-memory", "4G",
        "--num-executors", "${var.aws_core_instance_count}",
        "--conf", "spark.dynamicAllocation.enabled=false",
        "${format("s3://%s/benchmarks.jar", aws_s3_bucket.sw_bucket.bucket)}"]
    }
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
  provisioner "local-exec" {
    command = "sleep 60"
  }
  service_role = "${aws_iam_role.emr_role.arn}"
}

