#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd `dirname $0`/.. &&  pwd)
source $TOPDIR/bin/sparkling-env.sh
# Verify there is Spark installation
checkSparkHome
# Verify if correct Spark version is used
checkSparkVersion


if [ $1 ] && [[ ${1} != "--"* ]]; then
  SCRIPT=$1
  shift
else
  echo "Path to script must be set"
  exit 0
fi
SCRIPT_MASTER=${MASTER:-"$DEFAULT_MASTER"}
SCRIPT_DEPLOY_MODE="cluster"
SCRIPT_DEPLOY_MODE=${DEPLOY_MODE:-"client"} 
SCRIPT_DRIVER_MEMORY=${DRIVER_MEMORY:-$DEFAULT_DRIVER_MEMORY}
SCRIPT_H2O_SYS_OPS=${H2O_SYS_OPS:-""}

echo "---------"
echo "  Using script    (SCRIPT)       : $SCRIPT"
echo "  Using master    (MASTER)       : $SCRIPT_MASTER"
echo "  Deploy mode     (DEPLOY_MODE)  : $SCRIPT_DEPLOY_MODE"
echo "  Driver memory   (DRIVER_MEMORY): $SCRIPT_DRIVER_MEMORY"
echo "  H2O JVM options (H2O_SYS_OPS)  : $SCRIPT_H2O_SYS_OPS"
echo "---------"
export SPARK_PRINT_LAUNCH_COMMAND=1
VERBOSE=--verbose

if [ "$EXAMPLE_MASTER" == "yarn-client" ] || [ "$EXAMPLE_MASTER" == "yarn-cluster" ]; then
#EXAMPLE_DEPLOY_MODE does not have to be set when executing on yarn
VERBOSE=
 spark-submit \
 --master $SCRIPT_MASTER \
 --driver-memory $SCRIPT_DRIVER_MEMORY \
 --driver-java-options "$SCRIPT_H2O_SYS_OPS" \
 --py-files $PY_ZIP_FILE \
 --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m" \
 $VERBOSE \
 $SCRIPT \
  "$@"
else
VERBOSE=
 spark-submit \
 --master $SCRIPT_MASTER \
 --driver-memory $SCRIPT_DRIVER_MEMORY \
 --driver-java-options "$SCRIPT_H2O_SYS_OPS" \
 --deploy-mode $SCRIPT_DEPLOY_MODE \
 --py-files $PY_ZIP_FILE \
 --conf spark.driver.extraJavaOptions="-XX:MaxPermSize=384m" \
 $VERBOSE \
 $SCRIPT \
 "$@"
fi

