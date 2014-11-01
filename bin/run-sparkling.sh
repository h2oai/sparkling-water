#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd `dirname $0`/.. &&  pwd)
source $TOPDIR/bin/sparkling-env.sh

DRIVER_CLASS=water.SparklingWaterDriver

(
 cd $TOPDIR
 $SPARK_HOME/bin/spark-submit --class "$DRIVER_CLASS" $FAT_JAR_FILE
)
