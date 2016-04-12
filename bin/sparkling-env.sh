function checkSparkHome() {
  # Example class prefix
  if [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
    echo "Please setup SPARK_HOME variable to your Spark installation!"
    exit -1
  fi
}

function getMasterArg() {
    # Find master in arguments
    while [[ $# > 0 ]] 
    do
      case "$1" in
          --master*) shift; echo $1
      esac
      shift
    done
}

if [ -z $TOPDIR ]; then
  echo "Caller has to setup TOPDIR variable!"
  exit -1
fi

function checkSparkVersion() {
  installed_spark_version=$($SPARK_HOME/bin/spark-submit --version 2>&1 | grep version | sed -e "s/.*version //" | sed -e "s/\([0-9][0-9]*.[0-9][0-9]*\).*/\1/")
  if ! [[ "$SPARK_VERSION" =~ "$installed_spark_version".* ]]; then
    echo "You are trying to use Sparkling Water built for Spark ${SPARK_VERSION}, but your \$SPARK_HOME(=$SPARK_HOME) property points to Spark of version ${installed_spark_version}. Please ensure correct Spark is provided and re-run Sparkling Water."
    exit -1
  fi
}

# Version of this distribution
VERSION=$( cat $TOPDIR/gradle.properties | grep version | grep -v '#' | sed -e "s/.*=//" )
H2O_VERSION=$(cat $TOPDIR/gradle.properties | grep h2oMajorVersion | sed -e "s/.*=//")
H2O_BUILD=$(cat $TOPDIR/gradle.properties | grep h2oBuild | sed -e "s/.*=//")
H2O_NAME=$(cat $TOPDIR/gradle.properties | grep h2oMajorName | sed -e "s/.*=//")
SPARK_VERSION=$(cat $TOPDIR/gradle.properties | grep sparkVersion | sed -e "s/.*=//")
# Fat jar for this distribution
FAT_JAR="sparkling-water-assembly-$VERSION-all.jar"
FAT_JAR_FILE="$TOPDIR/assembly/build/libs/$FAT_JAR"
PY_EGG="pySparkling-${VERSION//-/_}-py2.7.egg"
PY_EGG_FILE="$TOPDIR/py/dist/$PY_EGG"

# Default master
DEFAULT_MASTER="local[*]"

# Setup loging and outputs
tmpdir="${TMPDIR:-"/tmp/"}/$USER/"
export SPARK_LOG_DIR="${tmpdir}spark/logs"
export SPARK_WORKER_DIR="${tmpdir}spark/work"
export SPARK_LOCAL_DIRS="${tmpdir}spark/work"

function banner() {
cat <<EOF

-----
  Spark master (MASTER)     : $MASTER
  Spark home   (SPARK_HOME) : $SPARK_HOME
  H2O build version         : ${H2O_VERSION}.${H2O_BUILD} ($H2O_NAME)
  Spark build version       : ${SPARK_VERSION}
----

EOF
}
