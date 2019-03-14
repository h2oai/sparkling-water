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

resource "aws_s3_bucket_object" "install_pysparkling" {
  bucket = "${aws_s3_bucket.sw_bucket.id}"
  key = "install_sw.sh"
  acl = "private"
  content = <<EOF

#!/bin/bash
set -x -e

sudo python3 -m pip install --upgrade colorama==0.3.9
sudo python3 -m pip install -U requests
sudo python3 -m pip install -U tabulate
sudo python3 -m pip install -U future
sudo python3 -m pip install -U six
sudo python3 -m pip install -U scikit-learn

sudo python2.7 -m pip install --upgrade colorama==0.3.9
sudo python2.7 -m pip install -U requests
sudo python2.7 -m pip install -U tabulate
sudo python2.7 -m pip install -U future
sudo python2.7 -m pip install -U six
sudo python2.7 -m pip install -U scikit-learn

mkdir -p /home/hadoop/h2o
cd /home/hadoop/h2o

wget https://s3.amazonaws.com/h2o-release/sparkling-water/rel-${var.sw_major_version}/${var.sw_patch_version}/sparkling-water-${var.sw_major_version}.${var.sw_patch_version}.zip

unzip -o sparkling-water-${var.sw_major_version}.${var.sw_patch_version}.zip 1> /dev/null & wait

PYSPARKLING_ZIP=$(find /home/hadoop/h2o/ -name h2o_pysparkling_*.zip)
sudo python3 -m pip install $PYSPARKLING_ZIP
sudo python2.7 -m pip install $PYSPARKLING_ZIP

export MASTER="yarn-client"
EOF
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
   sudo docker exec jupyterhub bash -c "echo $1:$(date +%s | sha256sum | base64 | head -c 32) | chpasswd"
   ADMIN_TOKEN=$(sudo docker exec jupyterhub /opt/conda/bin/jupyterhub token jovyan | tail -1)
   curl -XPOST --silent -k https://$(hostname):9443/hub/api/users/$1 -H "Authorization: token $ADMIN_TOKEN" | jq .
   curl -XPOST --silent -k https://$(hostname):9443/hub/api/users/$1/server -H "Authorization: token $ADMIN_TOKEN"
   echo $ADMIN_TOKEN | aws s3 cp - ${format("s3://%s/user.token", aws_s3_bucket.sw_bucket.bucket)} --acl private --content-type "text/plain"

    PYSPARKLING_ZIP=$(find /home/hadoop/h2o/ -name h2o_pysparkling_*.zip)
    SPARKLING_WATER_JAR=$(find /home/hadoop/h2o/ -name sparkling-water-assembly_2.11*-all.jar)
    # Disable Dynamic Allocation
    sudo -E sh -c "echo spark.dynamicAllocation.enabled false >> /etc/spark/conf/spark-defaults.conf"
    sudo -E sh -c "echo spark.jars   $SPARKLING_WATER_JAR >> /etc/spark/conf/spark-defaults.conf"
    sudo -E sh -c "echo spark.submit.pyFiles  $PYSPARKLING_ZIP >> /etc/spark/conf/spark-defaults.conf"
    sudo cp $SPARKLING_WATER_JAR /usr/lib/spark/jars/
  fi

EOF
}

data "aws_s3_bucket_object" "user_token" {
  bucket = "${aws_s3_bucket.sw_bucket.bucket}"
  key    = "user.token"
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
      path = "${format("s3://%s/install_sw.sh", aws_s3_bucket.sw_bucket.bucket)}"
      name = "Custom action"
    }
  ]

  step {
    action_on_failure = "TERMINATE_CLUSTER"
    name   = "Set up Jupyter and Spark Env"

    hadoop_jar_step {
      jar  = "${format("s3://%s.elasticmapreduce/libs/script-runner/script-runner.jar", var.aws_region)}"
      args = ["${format("s3://%s/setup_jupyter.sh", aws_s3_bucket.sw_bucket.bucket)}", "${var.jupyter_name}"]
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
    command = "sleep 20"
  }
  service_role = "${aws_iam_role.emr_role.arn}"
}
