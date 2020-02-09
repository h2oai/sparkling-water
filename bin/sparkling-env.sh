#!/usr/bin/env bash

function checkSparkHome() {
  # Example class prefix
  if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    if [ -z "$(which spark-submit)" ] ; then
      echo "Please setup SPARK_HOME variable to your Spark installation!"
      exit -1
    else
      echo
      echo "Using Spark defined on the system PATH=${PATH}"
      echo
    fi
  else
      echo
      echo "Using Spark defined in the SPARK_HOME=${SPARK_HOME} environmental property"
      echo
      export PATH=$SPARK_HOME/bin:$PATH
  fi
}

function removeArgWithParam() {
    filter=$1
    shift
    arg_list=""
    skip_next=0
    for arg in $@
    do
        shift
        [ $skip_next = 1 ] && skip_next=0 && continue
        [ "$arg" = "$filter" ] && skip_next=1 && continue
        arg_list="$arg_list $arg"
    done

    echo "$arg_list"
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

function getJarsArg() {
    while [[ $# -gt 0 ]]
    do
      case "$1" in
          --jars*) shift; echo "$1"
      esac
      shift
    done
}



function checkPythonPackages() {
    packages=$(pip list --format=freeze)
    error=0
    checkPythonPackage "$packages" "colorama" "0.3.8"
    checkPythonPackage "$packages" "requests"
    checkPythonPackage "$packages" "tabulate"
    checkPythonPackage "$packages" "future" "0.4.0"

    if [ $error == -1 ]; then
        exit -1
    fi
}

function checkPythonPackage() {
    res=$(echo $1 | tr " " "\n" | grep "^$2==*")
    if [ -z "$res" ]; then
        echo "\"$2\" package is not installed, please install it as: pip install $2"
        error=-1
    else
        if [ ! -z "$3" ]; then
            # check version
            major_min=$(echo $3 | cut -f1 -d.)
            minor_min=$(echo $3 | cut -f2 -d.)
            patch_min=$(echo $3 | cut -f3 -d.)

            current_version=$(echo $res | cut -f3 -d=)
            major_curr=$(echo $current_version | cut -f1 -d.)
            minor_curr=$(echo $current_version | cut -f2 -d.)
            patch_curr=$(echo $current_version | cut -f3 -d.)

            if [ $major_curr -lt $major_min ]; then
                echo "Please upgrade $2 to version at least $3 as: pip install --upgrade $2==$3" && exit
            elif [ $major_curr -gt $major_min ]; then
                return 0
            fi

            if [ $minor_curr -lt $minor_min ]; then
                echo "Please upgrade $2 to version at least $3 as: pip install --upgrade $2==$3" && exit
            elif [ $minor_curr -gt $minor_min ]; then
                return 0
            fi

            if [ $patch_curr -lt $patch_min ]; then
                echo "Please upgrade $2 to version at least $3 as: pip install --upgrade $2==$3" && exit
            fi

        fi

    fi
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
  installed_spark_version=$(spark-submit --version 2>&1 | grep version | grep -v Scala | sed -e "s/.*version //" | sed -e "s/\([0-9][0-9]*.[0-9][0-9]*\).*/\1/")
  if ! [[ "$SPARK_VERSION" =~ "$installed_spark_version".* ]]; then
    echo "You are trying to use Sparkling Water built for Spark ${SPARK_VERSION}, but your Spark is of version ${installed_spark_version}. Please ensure correct Spark is provided and re-run Sparkling Water."
    exit -1
  fi
}


# Disable grep options for this environment
export GREP_OPTIONS=
# Version of this distribution
PROP_FILE="$TOPDIR/gradle.properties"
export VERSION
VERSION=$(grep version "$PROP_FILE" | grep -v '#' | sed -e "s/.*=//" )
export H2O_VERSION
H2O_VERSION=$(grep h2oMajorVersion "$PROP_FILE" | sed -e "s/.*=//")
export H2O_BUILD
H2O_BUILD=$(grep h2oBuild "$PROP_FILE" | sed -e "s/.*=//")
export H2O_NAME
H2O_NAME=$(grep h2oMajorName "$PROP_FILE" | sed -e "s/.*=//")
export SPARK_VERSION=$(grep sparkVersion "$PROP_FILE" | sed -e "s/.*=//")
SCALA_VERSION=$(grep scalaBaseVersion "$PROP_FILE" | sed -e "s/.*=//" | cut -d . -f 1,2)
# Fat jar for this distribution
FAT_JAR="sparkling-water-assembly_$SCALA_VERSION-$VERSION-all.jar"
export FAT_JAR_FILE="$TOPDIR/jars/$FAT_JAR"
export MAJOR_VERSION
MAJOR_VERSION=$(echo "$VERSION" | cut -d . -f 1,2)
export PATCH_VERSION
PATCH_VERSION=$(echo "$VERSION" | cut -d . -f 3)
export SPARK_MAJOR_VERSION
SPARK_MAJOR_VERSION=$(echo "$SPARK_VERSION" | cut -d . -f 1,2)

PY_ZIP="h2o_pysparkling_${SPARK_MAJOR_VERSION}-${VERSION}.zip"
export PY_ZIP_FILE="$TOPDIR/py/build/dist/$PY_ZIP"
export AVAILABLE_H2O_DRIVERS
AVAILABLE_H2O_DRIVERS=$( [ -f "$TOPDIR/h2o_drivers.txt" ] && cat "$TOPDIR/h2o_drivers.txt" || echo "N/A" )

# Default master
export DEFAULT_MASTER="local[*]"
export DEFAULT_DRIVER_MEMORY=2G

export S3_RELEASE_BUCKET="https://h2o-release.s3.amazonaws.com/sparkling-water"

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
