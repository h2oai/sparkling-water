FROM docker.h2o.ai/opsh2oai/h2o-3-hadoop-hdp-2.2:46

ENV LANG 'C.UTF-8'
RUN locale

RUN \
    rm /etc/startup/40_hive_start && \
    rm /etc/startup/60_start_slapd

RUN apt-get update && apt-get install -y git curl

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

USER jenkins

###
### Warm up Gradle caches for Sparkling Water
###
RUN cd /home/jenkins && \
    git clone https://github.com/h2oai/sparkling-water.git && \
    cd sparkling-water && \
    ./gradlew build -x check -x :sparkling-water-py:build && \
    cd .. && rm -rf sparkling-water

USER root