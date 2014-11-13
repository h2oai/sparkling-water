#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd `dirname $0`/.. &&  pwd)
source $TOPDIR/bin/sparkling-env.sh
# Verify there is Spark installation
checkSparkHome

DRIVER_CLASS=water.SparklingWaterDriver

DRIVER_MEMORY=${DRIVER_MEMORY:-1G}
MASTER=${MASTER:-"local-cluster[3,2,1024]"}
VERBOSE=--verbose
VERBOSE=

# Show banner
banner 

(
 cd $TOPDIR
 $SPARK_HOME/bin/spark-submit "$@" $VERBOSE --driver-memory $DRIVER_MEMORY --master $MASTER --class "$DRIVER_CLASS" $FAT_JAR_FILE
)
