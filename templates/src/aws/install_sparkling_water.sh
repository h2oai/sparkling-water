#!/bin/bash
set -x -e


# AWS EMR bootstrap script
# for installing Sparkling Water on AWS EMR with Spark
#
##############################

## Python installations and libraries needed on the worker roles in order to get Sparkling Water & PySparkling working
sudo python -m pip install --upgrade pip==9.0.3
sudo python -m pip install --upgrade colorama==0.3.9

sudo ln -sf /usr/local/bin/pip2.7 /usr/bin/pip

# Install PySparkling Dependencies
sudo python -m pip install -U requests
sudo python -m pip install -U tabulate
sudo python -m pip install -U future
sudo python -m pip install -U six

# Install Scikit Learn
sudo python -m pip install -U scikit-learn

mkdir -p /home/hadoop/h2o
cd /home/hadoop/h2o

echo -e "\n Installing sparkling water version SUBST_SW_VERSION"

wget http://h2o-release.s3.amazonaws.com/sparkling-water/spark-SUBST_SPARK_MAJOR_VERSION/SUBST_S3_PATH/SUBST_SW_VERSION/sparkling-water-SUBST_SW_VERSION.zip &
wait

unzip -o sparkling-water-SUBST_SW_VERSION.zip 1> /dev/null &
wait

export MASTER="yarn-client"
