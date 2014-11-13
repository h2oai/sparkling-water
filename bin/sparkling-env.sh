function checkSparkHome() {
  # Example class prefix
  if [ ! -d "$SPARK_HOME" ]; then
    echo "Please setup SPARK_HOME variable to your Spark installation!"
    exit -1
  fi
}

if [ -z $TOPDIR ]; then
  echo "Caller has to setup TOPDIR variable!"
  exit -1
fi

# Version of this distribution
VERSION=$( cat $TOPDIR/gradle.properties | grep version | sed -e "s/.*=//" )
# Fat jar for this distribution
FAT_JAR="sparkling-water-assembly-$VERSION-all.jar"
FAT_JAR_FILE="$TOPDIR/assembly/build/libs/$FAT_JAR"

# Setup loging and outputs
tmpdir=${TMPDIR:-"/tmp/"}
export SPARK_LOG_DIR="${tmpdir}spark/logs"
export SPARK_WORKER_DIR="${tmpdir}spark/work"
export SPARK_LOCAL_DIRS="${tmpdir}spark/work"

function banner() {
cat <<EOF

-----
  Spark master (MASTER)     : $MASTER
  Spark home   (SPARK_HOME) : $SPARK_HOME
----

EOF
}
