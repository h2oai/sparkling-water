#!/usr/bin/env bash

#
# Prepares distribution package which can be uploaded into s3
#
# Expects prepared assembly.
#

set -e
set -u

# Current dir
TOPDIR=$(cd `dirname $0` &&  pwd)
source $TOPDIR/bin/sparkling-env.sh
DIST_DIR="$TOPDIR/dist/"
DIST_BUILD_DIR="${DIST_DIR}build/"
[ -d "$DIST_BUILD_DIR" ] && rm -rf "$DIST_BUILD_DIR"
mkdir "$DIST_BUILD_DIR" 2> /dev/null

cat > "$TOPDIR/demofiles.list" <<EOF
bin/
bin/launch-spark-cloud.sh
bin/run-example.sh
bin/sparkling-shell
bin/sparkling-env.sh
assembly/build/libs/sparkling-water-assembly-$VERSION-all.jar
examples/README.md
examples/scripts/dlDemo.script
examples/smalldata/allyears2k_headers.csv.gz
examples/smalldata/Chicago_Ohare_International_Airport.csv
examples/smalldata/prostate.csv
LICENSE
README.md
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

# Copy dist dir files
cat "$DIST_DIR/index.html" | sed -e "s/SUBST_PROJECT_VERSION/$VERSION/g" > "$DIST_BUILD_DIR/index.html"

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

