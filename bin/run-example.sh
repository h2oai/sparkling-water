#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd `dirname $0`/.. &&  pwd)
source $TOPDIR/bin/sparkling-env.sh

FAT_JAR="sparkling-water-assembly-$VERSION-all.jar"
# Example class prefix
if [ ! -d "$SPARK_HOME" ]; then
  echo "Please setup SPARK_HOME variable to your Spark installation!"
  exit -1
fi
# Example prefix
PREFIX=org.apache.spark.examples.h2o
# Name of default example
DEFAULT_EXAMPLE=AirlinesWithWeatherDemo

if [ $1 ]; then
  EXAMPLE=$PREFIX.$1
else
  EXAMPLE=$PREFIX.$DEFAULT_EXAMPLE
fi

EXAMPLE_MASTER=${MASTER:-"local-cluster[3,2,1024]"}
EXAMPLE_DEPLOY_MODE="cluster"
EXAMPLE_DEPLOY_MODE=${DEPLOY_MODE:-"client"} 
EXAMPLE_NUM_OF_H2O_WORKERS=${NUM_WORKERS:-3} # 2 real workers + 1 in driver
EXAMPLE_DRIVER_MEMORY=${DRIVER_MEMORY:-1G}
EXAMPLE_H2O_SYS_OPS=${H2O_SYS_OPS:-""}
tmpdir=${TMPDIR:-"/tmp/"}
export SPARK_LOG_DIR="${tmpdir}spark/logs"
export SPARK_WORKER_DIR="${tmpdir}spark/work"
export SPARK_LOCAL_DIRS="${tmpdir}spark/work"
#
echo "---------"
echo "  Using example                  : $EXAMPLE"
echo "  Using master    (MASTER)       : $EXAMPLE_MASTER"
echo "  Deploy mode     (DEPLOY_MODE)  : $EXAMPLE_DEPLOY_MODE"
echo "  Exp. workers    (NUM_WORKERS)  : $EXAMPLE_NUM_OF_H2O_WORKERS"
echo "  Driver memory   (DRIVER_MEMORY): $EXAMPLE_DRIVER_MEMORY"
echo "  H2O JVM options (H2O_SYS_OPS)  : $EXAMPLE_H2O_SYS_OPS"
echo "---------"
export SPARK_PRINT_LAUNCH_COMMAND=1
VERBOSE=--verbose
VERBOSE=
(
 cd $TOPDIR
 $SPARK_HOME/bin/spark-submit $VERBOSE --driver-memory $EXAMPLE_DRIVER_MEMORY --driver-java-options "$EXAMPLE_H2O_SYS_OPS -Dspark.h2o.workers=$EXAMPLE_NUM_OF_H2O_WORKERS" --master $EXAMPLE_MASTER --deploy-mode $EXAMPLE_DEPLOY_MODE --class $EXAMPLE $TOPDIR/assembly/build/libs/$FAT_JAR
)
