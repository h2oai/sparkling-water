#!/bin/bash

wget -q -O - https://pkg.jenkins.io/debian/jenkins-ci.org.key | apt-key add -
sh -c 'echo deb http://pkg.jenkins.io/debian-stable binary/ > /etc/apt/sources.list.d/jenkins.list'
add-apt-repository ppa:certbot/certbot --yes
apt update
apt install unzip openjdk-8-jre jenkins apache2 certbot -y

/home/ubuntu/.init/init-ssl.sh
/home/ubuntu/.init/install-plugins.sh bouncycastle-api matrix-auth ec2 authorize-project workflow-multibranch github aws-credentials workflow-cps-global-lib ws-cleanup github-branch-source basic-branch-build-strategies ansicolor timestamper docker-workflow amazon-ecr permissive-script-security blueocean

# Start And Configure Jenkins
echo 'export JAVA_ARGS="-Djava.awt.headless=true -Djenkins.install.runSetupWizard=false -Dpermissive-script-security.enabled=no_security"' >> /etc/default/jenkins
mkdir -p /var/lib/jenkins/init.groovy.d
cp /home/ubuntu/.init/init_jenkins.groovy /var/lib/jenkins/init.groovy.d

systemctl start jenkins
systemctl restart jenkins
