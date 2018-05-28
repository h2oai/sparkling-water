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

RUN apt-get -q -y install wget git curl gcc make
RUN apt-get -q -y install zlib1g-dev bzip2 libreadline-dev libssl-dev
# Install pyenv
ENV HOME  /home/jenkins
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

USER jenkins


RUN cd /home/jenkins/ && git clone git://github.com/yyuu/pyenv.git .pyenv
RUN pyenv install 3.6.5
RUN pyenv install 2.7.13
RUN pyenv global 3.6.5
RUN pyenv rehash

RUN cd /home/jenkins && git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv

RUN set -ex && \
    echo 'eval "$(pyenv init -)"' >> $HOME/.bashrc && \
    echo 'eval "$(pyenv virtualenv-init -)"' >> $HOME/.bashrc && \
    /bin/bash -c 'source $HOME/.bashrc'


RUN cd /home/jenkins && \
    pyenv virtualenv 2.7.13 sparkling-water-2.7 && \
    pip install future colorama request tabulate
RUN cd /home/jenkins && \
    pyenv virtualenv 3.6.5 sparkling-water-3.6 && \
    pip install future colorama request tabulate

# Try
RUN pyenv activate 2.7.13



###
###  Download Spark versions
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

# Warm up Gradle caches
RUN cd /home/jenkins && \
    git clone https://github.com/h2oai/sparkling-water.git && \
    cd sparkling-water && \
    ./gradlew build -x check -x :sparkling-water-py:build && \
    cd .. && rm -rf sparkling-water

