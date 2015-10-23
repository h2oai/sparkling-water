FROM ubuntu:precise

# Repo
#RUN echo "deb http://archive.ubuntu.com/ubuntu precise main universe" > /etc/apt/sources.list
# Java 7 installation from Oracle
RUN apt-get -y install python-software-properties
RUN add-apt-repository ppa:webupd8team/java

# Upgrade package index
RUN apt-get update 
# RUN apt-get -y upgrade

# automatically accept oracle license
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections

# and install java 7 oracle jdk
RUN apt-get -y install oracle-java7-installer && apt-get clean
RUN apt-get -y install oracle-java7-set-default

# Install additional tools
RUN apt-get -y install \
  less \
  curl \
  vim-tiny \
  sudo \
  openssh-server \
  unzip

# Install Spark 1.5.1
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.5.1-bin-hadoop2.4.tgz | tar -xz -C /opt && \
    ln -s /opt/spark-1.5.1-bin-hadoop2.4 /opt/spark && \
    mkdir /opt/spark/work && \
    chmod 0777 /opt/spark/work

# Install Sparkling water latest version
RUN curl -s http://h2o-release.s3.amazonaws.com/sparkling-water/rel-1.5/2/sparkling-water-1.5.2.zip --output sw.zip && \
  unzip sw.zip -d /opt/ && \
  ln -s /opt/sparkling-water-1.5.2 /opt/sparkling-water && \
  rm -f sw.zip

# Setup environment
ENV SPARK_HOME /opt/spark
ENV SPARKLING_WATER_HOME /opt/sparkling-water

WORKDIR ${SPARKLING_WATER_HOME}

