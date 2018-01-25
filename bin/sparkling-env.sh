#!/usr/bin/env bash

function checkSparkHome() {
  # Example class prefix
  if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    if [ -z "$(which spark-submit)" ] ; then
      echo "Please setup SPARK_HOME variable to your Spark installation!"
      exit -1
    fi
  else
      export PATH=$SPARK_HOME/bin:$PATH
  fi
}

function getMasterArg() {
    # Find master in arguments
    while [[ $# -gt 0 ]]
    do
      case "$1" in
          --master*) shift; echo "$1"
      esac
      shift
    done
}

if [ -z "$TOPDIR" ]; then
  echo "Caller has to setup TOPDIR variable!"
  exit -1
fi

function checkJava(){
if [ -z $(which java) ]; then
    echo "Java is not installed. Please install Java first before continuing with Sparkling Water."
    exit -1
fi
}


function checkSparkVersion() {
  checkJava
  installed_spark_version=$(spark-submit --version 2>&1 | grep version | grep -v Scala | sed -e "s/.*version //" | sed -e "s/\([0-9][0-9]*.[0-9][0-9]*\).*/\1/")
  if ! [[ "$SPARK_VERSION" =~ "$installed_spark_version".* ]]; then
    echo "You are trying to use Sparkling Water built for Spark ${SPARK_VERSION}, but your \$SPARK_HOME(=$SPARK_HOME) property points to Spark of version ${installed_spark_version}. Please ensure correct Spark is provided and re-run Sparkling Water."
    exit -1
  fi
}


# Disable grep options for this environment
export GREP_OPTIONS=
# Version of this distribution
PROP_FILE="$TOPDIR/gradle.properties"
VERSION=$(grep version "$PROP_FILE" | grep -v '#' | sed -e "s/.*=//" )
H2O_VERSION=$(grep h2oMajorVersion "$PROP_FILE" | sed -e "s/.*=//")
H2O_BUILD=$(grep h2oBuild "$PROP_FILE" | sed -e "s/.*=//")
H2O_NAME=$(grep h2oMajorName "$PROP_FILE" | sed -e "s/.*=//")
SPARK_VERSION=$(grep sparkVersion "$PROP_FILE" | sed -e "s/.*=//")
SCALA_VERSION=$(grep scalaBaseVersion "$PROP_FILE" | sed -e "s/.*=//" | cut -d . -f 1,2)
# Fat jar for this distribution
FAT_JAR="sparkling-water-assembly_$SCALA_VERSION-$VERSION-all.jar"
export FAT_JAR_FILE="$TOPDIR/assembly/build/libs/$FAT_JAR"
export MAJOR_VERSION
MAJOR_VERSION=$(echo "$VERSION" | cut -d . -f 1,2)
export PATCH_VERSION
PATCH_VERSION=$(echo "$VERSION" | cut -d . -f 3)

PY_ZIP="h2o_pysparkling_${MAJOR_VERSION}-${VERSION}.zip"
export PY_ZIP_FILE="$TOPDIR/py/build/dist/$PY_ZIP"
export AVAILABLE_H2O_DRIVERS
AVAILABLE_H2O_DRIVERS=$( [ -f "$TOPDIR/h2o_drivers.txt" ] && cat "$TOPDIR/h2o_drivers.txt" || echo "N/A" )

# Default master
export DEFAULT_MASTER="local[*]"
export DEFAULT_DRIVER_MEMORY=2G

# Setup loging and outputs
tmpdir="${TMPDIR:-"/tmp/"}/$USER/"
export SPARK_LOG_DIR="${tmpdir}spark/logs"
export SPARK_WORKER_DIR="${tmpdir}spark/work"
export SPARK_LOCAL_DIRS="${tmpdir}spark/work"

export S3_RELEASE_BUCKET="http://h2o-release.s3.amazonaws.com/sparkling-water"

function checkFatJarExists() {
if [ ! -f "$FAT_JAR_FILE" ]; then
    echo
    echo "Sparkling Water assembly jar does not exist at: $FAT_JAR_FILE. Can not continue!"
    echo
    exit -1
fi
}

function checkPyZipExists() {
if [ ! -f "$PY_ZIP_FILE" ]; then
    echo
    echo "PySparkling zip distribution does not exist at: $PY_ZIP_FILE. Can not continue!"
    echo
    exit -1
fi
}

function banner() {
cat <<EOF

-----
  Spark master (MASTER)     : $MASTER
  Spark home   (SPARK_HOME) : $SPARK_HOME
  H2O build version         : ${H2O_VERSION}.${H2O_BUILD} ($H2O_NAME)
  Sparkling Water version   : ${VERSION}
  Spark build version       : ${SPARK_VERSION}
  Scala version             : ${SCALA_VERSION}
----

EOF
}
