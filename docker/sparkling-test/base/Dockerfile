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

# Install Spark
RUN curl -s http://d3kbcqa49mib13.cloudfront.net/spark-1.1.0-bin-cdh4.tgz | tar -xz -C /opt/
RUN cd /opt && ln -s spark-1.1.0-bin-cdh4 spark
RUN mkdir /opt/spark/work 
RUN chmod 0777 /opt/spark/work

# Install Sparkling water
RUN curl -s http://h2o-release.s3.amazonaws.com/sparkling-water/master/54/sparkling-water-0.2.1-54.zip > sw.zip && unzip sw.zip -d /opt/
RUN cd /opt && ln -s sparkling-water-0.2.1-54 sparkling-water

# Setup environment
ENV SPARK_HOME /opt/spark
ENV SPARKLING_WATER_HOME /opt/sparkling-water

WORKDIR ${SPARKLING_WATER_HOME}

