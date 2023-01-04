#!/bin/bash

sudo yum install docker git -y
sudo amazon-linux-extras install java-openjdk11 -y
sudo yum -y update --security
sudo usermod -a -G docker ec2-user
sudo chkconfig docker on
sudo groupadd -g 2117 jenkins
sudo useradd jenkins --uid 2117 --gid 2117 -d /home/jenkins
sudo usermod -a -G docker jenkins
sudo mkdir -p /home/jenkins/.ssh
