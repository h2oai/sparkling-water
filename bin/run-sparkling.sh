#!/usr/bin/env bash
set -e
# Current dir
TOPDIR=$(cd `dirname $(readlink "$0" || echo "$0")`/.. ; pwd -P)
source $TOPDIR/bin/sparkling-env.sh
# Verify there is Spark installation
checkSparkHome

DRIVER_CLASS=water.SparklingWaterDriver

(
 cd $TOPDIR
 $SPARK_HOME/bin/spark-submit --class "$DRIVER_CLASS" $FAT_JAR_FILE
)
