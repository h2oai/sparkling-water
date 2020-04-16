#!/bin/bash

apt-key adv --fetch-keys https://pkg.jenkins.io/debian/jenkins.io.key
add-apt-repository ppa:certbot/certbot --yes
sh -c 'echo deb http://pkg.jenkins.io/debian-stable binary/ > /etc/apt/sources.list.d/jenkins.list'
apt update
apt install unzip openjdk-8-jre jenkins apache2 certbot -y

./init-ssl.sh
./installPlugins.sh matrix-auth ec2 authorize-project workflow-multibranch github

# Start And Configure Jenkins
echo 'export JAVA_ARGS="-Djava.awt.headless=true -Djenkins.install.runSetupWizard=false"' >> /etc/default/jenkins
cp ./init-credentials.groovy /var/lib/jenkins/init.groovy.d/
cp ./init-security.groovy /var/lib/jenkins/init.groovy.d/

mkdir -p /var/lib/jenkins/init.groovy.d
systemctl start jenkins
systemctl restart jenkins
