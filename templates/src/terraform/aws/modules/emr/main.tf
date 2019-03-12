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
  public_key = "ssh-rsa ${var.aws_ssh_public_key}"
}

resource "aws_s3_bucket" "sw_bucket" {
  acl = "private"
  force_destroy = true
  tags = {
    Name        = "SparklingWaterDeploymentBucket"
  }
}

resource "aws_s3_bucket_object" "juputer_init_script" {
  bucket = "${aws_s3_bucket.sw_bucket.id}"
  key    = "setup_jupyter.sh"
  acl = "private"
  content = <<EOF

  #!/bin/bash
  set -x -e

  IS_MASTER=false
  if [ -f /mnt/var/lib/info/instance.json ]
  then
   IS_MASTER=`cat /mnt/var/lib/info/instance.json | grep "isMaster" | cut -f2 -d: | tr -d " "`
  fi

  if [ "$IS_MASTER" = true ]; then
   sudo docker exec jupyterhub useradd -m -s /bin/bash -N $1
   sudo docker exec jupyterhub bash -c "echo $1:$2 | chpasswd"
   ADMIN_TOKEN=$(sudo docker exec jupyterhub /opt/conda/bin/jupyterhub token jovyan | tail -1)
   curl -XPOST --silent -k https://$(hostname):9443/hub/api/users/$1 -H "Authorization: token $ADMIN_TOKEN" | jq .
  fi

EOF
}


resource "aws_emr_cluster" "sparkling-water-cluster" {
  name = "Sparkling-Water"
  release_label = "${var.aws_emr_version}"
  log_uri = "s3://${aws_s3_bucket.sw_bucket.bucket}/"
  applications = [
    "Spark",
    "Hadoop",
    "JupyterHub"]

  ec2_attributes {
    subnet_id = "${data.aws_subnet.main.id}"
    key_name = "${aws_key_pair.key.key_name}"
    emr_managed_master_security_group = "${aws_security_group.slave.id}"
    emr_managed_slave_security_group = "${aws_security_group.master.id}"
    instance_profile = "${aws_iam_instance_profile.emr_ec2_instance_profile.arn}"
  }

  master_instance_type = "${var.aws_instance_type}"
  core_instance_type = "${var.aws_instance_type}"
  core_instance_count = "${var.aws_core_instance_count}"

  tags {
    name = "SparklingWater"
  }

  bootstrap_action = [
    {
      path = "${format("s3://h2o-release/sparkling-water/rel-%s/%s/templates/aws/install_sparkling_water_%s.%s.sh",
          var.sw_major_version, var.sw_patch_version, var.sw_major_version, var.sw_patch_version)}"
      name = "Custom action"
    }
  ]

  step {
    action_on_failure = "TERMINATE_CLUSTER"
    name   = "Add Jupyter Notebook User"

    hadoop_jar_step {
      jar  = "${format("s3://%s.elasticmapreduce/libs/script-runner/script-runner.jar", var.aws_region)}"
      args = ["${format("s3://%s/setup_jupyter.sh", aws_s3_bucket.sw_bucket.bucket)}", "${var.jupyter_name}", "${var.jupyter_pass}"]
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
