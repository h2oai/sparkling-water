FROM ubuntu:16.04

ARG JENKINS_UID='2117'
ARG JENKINS_GID='2117'

ENV LANG 'C.UTF-8'
RUN locale

###
### Add the Jenkins user
###

RUN \
  groupadd -g ${JENKINS_GID} jenkins && \
  adduser --uid ${JENKINS_UID} -gid ${JENKINS_GID} --disabled-password --gecos "" jenkins

###
### Install Java and all Ubuntu dependencies
###

RUN \
    apt-get update && \
    apt-get -q -y update && \
    apt-get -y install software-properties-common && \
    add-apt-repository -y ppa:deadsnakes && \
    echo "oracle-java8-installer shared/accepted-oracle-license-v1-1 select true" | debconf-set-selections && \
    echo "deb http://ppa.launchpad.net/webupd8team/java/ubuntu xenial main" > /etc/apt/sources.list.d/webupd8team-java-trusty.list && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys EEA14886 && \
    apt-get update -q -y && \
    apt-get install -y --no-install-recommends oracle-java8-installer oracle-java8-set-default wget git curl python2.7 python2.7-dev python3.6 python3.6-dev python-virtualenv virtualenv && \
    apt-get clean all

###
### Prepare directories for Python Virtual environments
###

RUN mkdir /envs && chown jenkins:jenkins /envs

###
### Root is necessary for apt-get install without password, switch to jenkins user to have correct permissions
###

USER jenkins

###
###  Prepare Python environments 2.7 & 3.6
###

RUN \
    virtualenv -p python2.7 /envs/h2o_env_python2.7 && \
    . /envs/sparkling-water2.7/bin/activate && \
    pip install future colorama request tabulate && \
    deactivate && \
    virtualenv -p python3.6 /envs/h2o_env_python3.6 && \
    . /envs/sparkling-water3.6/bin/activate && \
    pip install future colorama request tabulate && \
    deactivate


###
###  Prepare Spark
###

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.6.tgz  && \
    mkdir -p spark-2.3.0-bin-hadoop2.6 &&  \
    tar zxvf spark-2.3.0-bin-hadoop2.6.tgz -C spark-2.3.0-bin-hadoop2.6 --strip-components 1 && \
    rm -rf spark-2.3.0-bin-hadoop2.6.tgz

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.6.tgz  && \
    mkdir -p spark-2.2.1-bin-hadoop2.6 &&  \
    tar zxvf spark-2.2.1-bin-hadoop2.6.tgz -C spark-2.2.1-bin-hadoop2.6 --strip-components 1 && \
    rm -rf spark-2.2.1-bin-hadoop2.6.tgz
 
RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.1.2/spark-2.1.2-bin-hadoop2.6.tgz  && \
    mkdir -p spark-2.1.2-bin-hadoop2.6 &&  \
    tar zxvf spark-2.1.2-bin-hadoop2.6.tgz -C spark-2.1.2-bin-hadoop2.6 --strip-components 1 && \
    rm -rf spark-2.1.2-bin-hadoop2.6.tgz   
    
ENV SPARK_HOME_2_3_0 /home/jenkins/spark-2.3.0-bin-hadoop2.6
ENV SPARK_HOME_2_2_1 /home/jenkins/spark-2.2.1-bin-hadoop2.6
ENV SPARK_HOME_2_1_2 /home/jenkins/spark-2.1.2-bin-hadoop2.6

###
### Warm up Gradle caches for Sparkling Water
###
RUN cd /home/jenkins && \
    git clone https://github.com/h2oai/sparkling-water.git && \
    cd sparkling-water && \
    ./gradlew build -x check -x :sparkling-water-py:build && \
    cd .. && rm -rf sparkling-water

