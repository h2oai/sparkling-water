#!/usr/bin/env bash

#
# Prepares distribution package which can be uploaded into s3
#
# Expects prepared assembly.
#

set -e
set -u
set -x

# Current dir
TOPDIR=$(cd `dirname $0` &&  pwd)
source $TOPDIR/bin/sparkling-env.sh
DIST_DIR="$TOPDIR/dist/"
DIST_BUILD_DIR="${DIST_DIR}build/"
[ -d "$DIST_BUILD_DIR" ] && rm -rf "$DIST_BUILD_DIR"
[ -d "$DIST_BUILD_DIR" ] || mkdir "$DIST_BUILD_DIR" 
[ -d "private" ] || mkdir private 

cat > "$TOPDIR/demofiles.list" <<EOF
bin/
$(find bin -type f)
assembly/build/libs/sparkling-water-assembly-$VERSION-all.jar
examples/README.md
examples/scripts/chicagoCrimeSmall.script.scala
examples/scripts/chicagoCrimeSmallShell.script.scala
examples/scripts/mlconf_2015_hamSpam.script.scala
examples/scripts/StrataAirlines.script.scala
examples/scripts/craigslistJobTitles.script.scala
examples/smalldata/allyears2k_headers.csv.gz
examples/smalldata/Chicago_Ohare_International_Airport.csv
examples/smalldata/prostate.csv
examples/smalldata/year2005.csv.gz
examples/smalldata/smsData.txt
examples/smalldata/chicagoAllWeather.csv
examples/smalldata/chicagoCensus.csv
examples/smalldata/chicagoCrimes10k.csv
examples/smalldata/craigslistJobTitles.csv
docker/
docker/README.md
docker/bin/
docker/bin/run-example.sh
docker/bin/sparkling-shell
docker/build.sh
docker/sparkling-test/
docker/sparkling-test/build.sh
docker/sparkling-test/base/
docker/sparkling-test/base/Dockerfile
$(find py/dist/ -type f -name '*.egg')
$(find py/examples/ -type f)
$(find py/pysparkling -type f -name '*.py')
LICENSE
README.md
DEVEL.md
CHANGELOG.md
gradle.properties
EOF

# Destination folder for ziping
DEST_DIRNAME="sparkling-water-$VERSION"
DEST_DIR="$TOPDIR/private/$DEST_DIRNAME"
[ -d "$DEST_DIR" ] && rm -rf "$DEST_DIR"

# Source and destination folders for Scaladoc
SCALADOC_SRC_DIR="$TOPDIR/core/build/docs/scaladoc/"
SCALADOC_DST_DIR="$DIST_BUILD_DIR/scaladoc/"

# Resulting zip file
ZIP_NAME="sparkling-water-$VERSION.zip" 
ZIP_FILE="$DIST_BUILD_DIR/$ZIP_NAME"
[ -f "$ZIP_FILE" ] && rm "$ZIP_FILE"

# Make distribution package and put it into dist directory
rsync -rtvW --files-from "$TOPDIR/demofiles.list" "$TOPDIR/" "$DEST_DIR/"
( 
 cd private
 zip -r "$ZIP_FILE" "$DEST_DIRNAME"
)
# Copy scaladoc
rsync -rtvW "$SCALADOC_SRC_DIR" "$SCALADOC_DST_DIR"

GITHASH=`git rev-parse --verify HEAD`

if [ "${H2O_NAME}" == "master" ]; then
  H2O_BRANCH_NAME="master"
else
  H2O_BRANCH_NAME="rel-${H2O_NAME}"
fi

H2O_PROJECT_VERSION=${H2O_VERSION}.${H2O_BUILD}
H2O_BUILD_NUMBER=${H2O_BUILD}

# Copy dist dir files
cat "$DIST_DIR/index.html" \
  | sed -e "s/SUBST_PROJECT_VERSION/$VERSION/g"\
  | sed -e "s/SUBST_PROJECT_GITHASH/${GITHASH}/g"\
  | sed -e "s/SUBST_H2O_VERSION/${H2O_VERSION}/g"\
  | sed -e "s/SUBST_H2O_BUILD/${H2O_BUILD}/g"\
  | sed -e "s/SUBST_H2O_NAME/${H2O_NAME}/g"\
  | sed -e "s/SUBST_SPARK_VERSION/${SPARK_VERSION}/g"\
  | sed -e "s/SUBST_H2O_BRANCH_NAME/${H2O_BRANCH_NAME}/g"\
  > "$DIST_BUILD_DIR/index.html"

# Create json metadata file.
cat "$DIST_DIR/buildinfo.json" \
  | sed -e "s/SUBST_BUILD_TIME_MILLIS/${BUILD_TIME_MILLIS}/g" \
  | sed -e "s/SUBST_BUILD_TIME_ISO8601/${BUILD_TIME_ISO8601}/g" \
  | sed -e "s/SUBST_BUILD_TIME_LOCAL/${BUILD_TIME_LOCAL}/g" \
  \
  | sed -e "s/SUBST_PROJECT_VERSION/${VERSION}/g" \
  | sed -e "s/SUBST_LAST_COMMIT_HASH/${GITHASH}/g" \
  \
  | sed -e "s/SUBST_H2O_NAME/${H2O_NAME}/g"\
  | sed -e "s/SUBST_H2O_VERSION/${H2O_VERSION}/g"\
  \
  | sed -e "s/SUBST_H2O_PROJECT_VERSION/${H2O_PROJECT_VERSION}/g"\
  | sed -e "s/SUBST_H2O_BRANCH_NAME/${H2O_BRANCH_NAME}/g"\
  | sed -e "s/SUBST_H2O_BUILD_NUMBER/${H2O_BUILD_NUMBER}/g"\
  \
  | sed -e "s/SUBST_SPARK_VERSION/${SPARK_VERSION}/g"\
  \
  > "$DIST_BUILD_DIR/buildinfo.json"

exit 0

# Cleanup
rm $TOPDIR/demofiles.list
rm -rf $DEST_DIR

# Prepare a zip file with Spark distribution
ZIPNAME_WITH_SPARK=$(echo $ZIPNAME | sed "s/water/water-with-spark/")
cp $ZIPNAME $ZIPNAME_WITH_SPARK

SPARK_DIST="spark-1.1.0-bin-cdh4"
SPARK_EXT="tgz"
(
 cd private
 tar -zxvf "${SPARK_DIST}.${SPARK_EXT}"
 zip -r -u ../$ZIPNAME_WITH_SPARK $SPARK_DIST
)

