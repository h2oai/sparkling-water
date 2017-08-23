#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd "$(dirname "$0")/.."; pwd)

source "$TOPDIR/bin/sparkling-env.sh"
# Verify there is Spark installation
checkSparkHome
# Check sparkling water assembly Jar exists
checkFatJarExists
# Example prefix
PREFIX=org.apache.spark.examples.h2o
# Name of default example
DEFAULT_EXAMPLE=AirlinesWithWeatherDemo2

if [ "$1" ] && [[ ${1} != "--"* ]]; then
  EXAMPLE=$PREFIX.$1
  shift
else
  EXAMPLE=$PREFIX.$DEFAULT_EXAMPLE
fi

EXAMPLE_MASTER=${MASTER:-"$DEFAULT_MASTER"}
EXAMPLE_DEPLOY_MODE="cluster"
EXAMPLE_DEPLOY_MODE=${DEPLOY_MODE:-"client"} 
EXAMPLE_DRIVER_MEMORY=${DRIVER_MEMORY:-$DEFAULT_DRIVER_MEMORY}
EXAMPLE_H2O_SYS_OPS=${H2O_SYS_OPS:-""}

echo "---------"
echo "  Using example                  : $EXAMPLE"
echo "  Using master    (MASTER)       : $EXAMPLE_MASTER"
echo "  Deploy mode     (DEPLOY_MODE)  : $EXAMPLE_DEPLOY_MODE"
echo "  Driver memory   (DRIVER_MEMORY): $EXAMPLE_DRIVER_MEMORY"
echo "  H2O JVM options (H2O_SYS_OPS)  : $EXAMPLE_H2O_SYS_OPS"
echo "---------"
export SPARK_PRINT_LAUNCH_COMMAND=1
VERBOSE= #--verbose

# Derive actual spark.driver.extraJavaOptions value
if [ -f "$SPARK_HOME/conf/spark-defaults.conf" ]; then
    EXTRA_DRIVER_PROPS=$(grep "^spark.driver.extraJavaOptions" "$SPARK_HOME"/conf/spark-defaults.conf 2>/dev/null | sed -e 's/spark.driver.extraJavaOptions//' )
fi

spark-submit --class $EXAMPLE \
--master "$EXAMPLE_MASTER" \
--driver-memory "$EXAMPLE_DRIVER_MEMORY" \
--driver-java-options "$EXAMPLE_H2O_SYS_OPS" \
--deploy-mode "$EXAMPLE_DEPLOY_MODE" \
--conf spark.driver.extraJavaOptions="$EXTRA_DRIVER_PROPS" \
$VERBOSE "$FAT_JAR_FILE" "$@"
