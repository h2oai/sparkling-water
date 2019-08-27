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

resource "aws_s3_bucket" "deployment_bucket" {
  acl = "public-read"
  force_destroy = true
  tags = {
    Name= "SparklingWaterBenchmarksDeploymentBucket"
  }
}

resource "aws_s3_bucket_policy" "read_objects" {
  bucket = "${aws_s3_bucket.deployment_bucket.id}"

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "ReadObjectsPolicy",
  "Statement": [
    {
      "Sid": "PublicReadForGetBucketObjects",
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${aws_s3_bucket.deployment_bucket.bucket}/public-read/*"
    }
  ]
}
POLICY
}

resource "aws_s3_bucket_object" "benchmarks_jar" {
  bucket = "${aws_s3_bucket.deployment_bucket.bucket}"
  key = "benchmarks.jar"
  acl = "private"
  source = "${var.sw_package_file}"
}

resource "aws_s3_bucket_object" "h2o_jar" {
  bucket = "${aws_s3_bucket.deployment_bucket.bucket}"
  key = "h2o.jar"
  acl = "private"
  source = "${var.h2o_jar_file}"
}

resource "aws_s3_bucket_object" "run_benchmarks_script" {
  bucket = "${aws_s3_bucket.deployment_bucket.id}"
  key    = "run_benchmarks.sh"
  acl = "private"
  content = <<EOF

  #!/bin/bash
  set -x -e

  function runBenchmarks {
    spark-submit \
      --class ai.h2o.sparkling.benchmarks.Runner \
      --master "$1" \
      --driver-memory "$3" \
      --executor-memory "$4" \
      --deploy-mode client \
      --num-executors ${var.aws_core_instance_count} \
      --conf "spark.dynamicAllocation.enabled=false" \
      --conf "spark.ext.h2o.backend.cluster.mode=$2" \
      --conf "spark.ext.h2o.external.cluster.size=${var.aws_core_instance_count}" \
      --conf "spark.ext.h2o.hadoop.memory=$4" \
      --conf "spark.ext.h2o.external.start.mode=auto" \
      ${format("s3://%s/benchmarks.jar", aws_s3_bucket.deployment_bucket.bucket)} \
      -o /home/hadoop/results
  }


  runBenchmarks "yarn" "internal" "8G" "8G"
  aws s3 cp ${format("s3://%s/h2o.jar", aws_s3_bucket.deployment_bucket.bucket)} /home/hadoop/h2o.jar
  export H2O_EXTENDED_JAR=/home/hadoop/h2o.jar
  runBenchmarks "yarn" "external" "8G" "4G"
  runBenchmarks "local" "internal" "8G" "8G"

  tar -zcvf /home/hadoop/results.tar.gz -C /home/hadoop/results .
  aws s3 cp /home/hadoop/results.tar.gz ${format("s3://%s/public-read/results.tar.gz", aws_s3_bucket.deployment_bucket.bucket)}
  touch /home/hadoop/finished
  aws s3 cp /home/hadoop/finished ${format("s3://%s/public-read/finished", aws_s3_bucket.deployment_bucket.bucket)}

EOF
}

resource "aws_emr_cluster" "sparkling-water-cluster" {
  name = "Sparkling-Water-Benchmarks"
  release_label = "${var.aws_emr_version}"
  log_uri = "s3://${aws_s3_bucket.deployment_bucket.bucket}/"
  applications = ["Spark", "Hadoop"]
  depends_on = [
    aws_s3_bucket_object.benchmarks_jar,
    aws_s3_bucket_object.h2o_jar,
    aws_s3_bucket_object.run_benchmarks_script
  ]

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
    action_on_failure = "CONTINUE"
    name = "ExecuteBenchmarks"

    hadoop_jar_step {
      jar  = "${format("s3://%s.elasticmapreduce/libs/script-runner/script-runner.jar", var.aws_region)}"
      args = ["${format("s3://%s/run_benchmarks.sh", aws_s3_bucket.deployment_bucket.bucket)}"]
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
  service_role = "${aws_iam_role.emr_role.arn}"
}
