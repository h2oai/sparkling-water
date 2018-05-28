FROM ubuntu:16.04

ARG JENKINS_UID='2117'
ARG JENKINS_GID='2117'

ENV LANG 'C.UTF-8'
RUN locale

# Add the Jenkins user
RUN \
  groupadd -g ${JENKINS_GID} jenkins && \
  adduser --uid ${JENKINS_UID} -gid ${JENKINS_GID} --disabled-password --gecos "" jenkins

RUN apt-get update && \
    apt-get -q -y update && \
    apt-get -y install software-properties-common && \
    echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
    echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" > /etc/apt/sources.list.d/webupd8team-java-trusty.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886 && \
    apt-get update -q -y && \
    apt-get install -y --no-install-recommends oracle-java8-installer oracle-java8-set-default && \
    apt-get clean all && \
    apt-get install oracle-java8-set-default

RUN apt-get -q -y install wget git curl

USER jenkins

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.3.0-bin-hadoop2.7 &&  \
    tar zxvf spark-2.3.0-bin-hadoop2.7.tgz -C spark-2.3.0-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.3.0-bin-hadoop2.7.tgz

ENV SPARK_HOME_2_3_0 /home/jenkins/spark-2.3.0-bin-hadoop2.7

# Warm up Gradle caches
ENV cd /home/jenkins && \
    git clone https://github.com/h2oai/sparkling-water.git && \
    cd sparkling-water && \
    ./gradlew build -x check -x :sparkling-water-py:build && \
    cd .. && rm -rf sparkling-water

