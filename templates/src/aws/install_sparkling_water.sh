#!/bin/bash
set -x -e


# AWS EMR bootstrap script
# for installing Sparkling Water on AWS EMR with Spark
#
##############################

## Python installations and libraries needed on the worker roles in order to get PySparkling working

sudo python -m pip install --upgrade pip
sudo python -m pip install --upgrade colorama

sudo ln -sf /usr/local/bin/pip2.7 /usr/bin/pip

#mkdir -p /home/h2o

sudo pip install -U requests
sudo pip install -U tabulate
sudo pip install -U future
sudo pip install -U six

#Scikit Learn on the nodes
sudo pip install -U scikit-learn

mkdir -p /home/hadoop/h2o
cd /home/hadoop/h2o

echo -e "\n Installing sparkling water version SUBST_MAJOR_VERSION.SUBST_MINOR_VERSION "

wget http://h2o-release.s3.amazonaws.com/sparkling-water/SUBST_BRANCH_NAME/SUBST_MINOR_VERSION/sparkling-water-SUBST_MAJOR_VERSION.SUBST_MINOR_VERSION.zip &
wait

unzip -o sparkling-water-SUBST_MAJOR_VERSION.SUBST_MINOR_VERSION.zip 1> /dev/null &
wait

export MASTER="yarn-client"

