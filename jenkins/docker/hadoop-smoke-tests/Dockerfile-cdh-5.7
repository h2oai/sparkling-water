FROM harbor.h2o.ai/opsh2oai/h2o-3-hadoop-cdh-5.7:58

ENV LANG 'C.UTF-8'
RUN locale

RUN rm -rf /etc/hadoop/conf/yarn-site.xml
COPY ../conf/yarn-site.xml /etc/hadoop/conf/yarn-site.xml

RUN \
    rm /etc/startup/70_start_slapd

RUN apt-get update && apt-get install -y git curl zip s3cmd r-base r-base-dev libxml2-dev libssl-dev libcurl4-openssl-dev

RUN \
    R -e 'install.packages("xml2", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("openssl", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("httr", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("httr", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("bitops", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("RCurl", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("jsonlite", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("testthat", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("dplyr", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("devtools", repos = "http://cran.us.r-project.org")' && \
    R -e 'install.packages("sparklyr", repos = "http://cran.us.r-project.org")'


###
###  Prepare Spark
###

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.4.0-bin-hadoop2.7 &&  \
    tar zxvf spark-2.4.0-bin-hadoop2.7.tgz -C spark-2.4.0-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.4.0-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.3.2/spark-2.3.2-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.3.2-bin-hadoop2.7 &&  \
    tar zxvf spark-2.3.2-bin-hadoop2.7.tgz -C spark-2.3.2-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.3.2-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.3.1-bin-hadoop2.7 &&  \
    tar zxvf spark-2.3.1-bin-hadoop2.7.tgz -C spark-2.3.1-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.3.1-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.3.0-bin-hadoop2.7 &&  \
    tar zxvf spark-2.3.0-bin-hadoop2.7.tgz -C spark-2.3.0-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.3.0-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.2.2/spark-2.2.2-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.2.2-bin-hadoop2.7 &&  \
    tar zxvf spark-2.2.2-bin-hadoop2.7.tgz -C spark-2.2.2-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.2.2-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.2.1/spark-2.2.1-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.2.1-bin-hadoop2.7 &&  \
    tar zxvf spark-2.2.1-bin-hadoop2.7.tgz -C spark-2.2.1-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.2.1-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.2.0/spark-2.2.0-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.2.0-bin-hadoop2.7 &&  \
    tar zxvf spark-2.2.0-bin-hadoop2.7.tgz -C spark-2.2.0-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.2.0-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://mirrors.ocf.berkeley.edu/apache/spark/spark-2.1.3/spark-2.1.3-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.1.3-bin-hadoop2.7 &&  \
    tar zxvf spark-2.1.3-bin-hadoop2.7.tgz -C spark-2.1.3-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.1.3-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.1.2/spark-2.1.2-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.1.2-bin-hadoop2.7 &&  \
    tar zxvf spark-2.1.2-bin-hadoop2.7.tgz -C spark-2.1.2-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.1.2-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.1.1-bin-hadoop2.7 &&  \
    tar zxvf spark-2.1.1-bin-hadoop2.7.tgz -C spark-2.1.1-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.1.1-bin-hadoop2.7.tgz

RUN cd /home/jenkins && \
    wget http://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0-bin-hadoop2.7.tgz  && \
    mkdir -p spark-2.1.0-bin-hadoop2.7 &&  \
    tar zxvf spark-2.1.0-bin-hadoop2.7.tgz -C spark-2.1.0-bin-hadoop2.7 --strip-components 1 && \
    rm -rf spark-2.1.0-bin-hadoop2.7.tgz

ENV SPARK_HOME_2_4_0 /home/jenkins/spark-2.4.0-bin-hadoop2.7
ENV SPARK_HOME_2_3_2 /home/jenkins/spark-2.3.2-bin-hadoop2.7
ENV SPARK_HOME_2_3_1 /home/jenkins/spark-2.3.1-bin-hadoop2.7
ENV SPARK_HOME_2_3_0 /home/jenkins/spark-2.3.0-bin-hadoop2.7
ENV SPARK_HOME_2_2_1 /home/jenkins/spark-2.2.1-bin-hadoop2.7
ENV SPARK_HOME_2_2_0 /home/jenkins/spark-2.2.0-bin-hadoop2.7
ENV SPARK_HOME_2_1_2 /home/jenkins/spark-2.1.2-bin-hadoop2.7
ENV SPARK_HOME_2_1_1 /home/jenkins/spark-2.1.1-bin-hadoop2.7
ENV SPARK_HOME_2_1_0 /home/jenkins/spark-2.1.0-bin-hadoop2.7

# Change permissions so we can install R packages
RUN chmod 777 /usr/local/lib/R/site-library

USER jenkins

ENV USER jenkins