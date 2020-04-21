#!/bin/bash

sudo yum install docker git java-1.8.0-openjdk -y
sudo alternatives --set java /usr/lib/jvm/jre-1.8.0-openjdk.x86_64/bin/java
sudo usermod -a -G docker ec2-user
sudo chkconfig docker on
sudo groupadd -g 2117 jenkins
sudo useradd jenkins --uid 2117 --gid 2117 -d /home/jenkins
sudo usermod -a -G docker jenkins
sudo mkdir -p /home/jenkins/.ssh
