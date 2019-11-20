#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd "$(dirname "$0")/.." || exit; pwd)

source "$TOPDIR/bin/sparkling-env.sh"

( cd "$SPARK_HOME" && docker-image-tool.sh -t "$SPARK_VERSION" build )

echo "Creating Working Directory"
WORKDIR=$(mktemp -d)
echo "Working directory created: $WORKDIR"

K8DIR="$TOPDIR/kubernetes"
cp "$K8DIR/Dockerfile-Python" "$WORKDIR"
cp "$K8DIR/Dockerfile-R" "$WORKDIR"
cp "$K8DIR/Dockerfile-Scala" "$WORKDIR"

echo "Bulding Docker Image for Sparkling Water(Scala) ..."
cp "$FAT_JAR_FILE" "$WORKDIR"
docker build -t "sparkling-water-scala:$VERSION" -f "$WORKDIR/Dockerfile-Scala" "$WORKDIR"
echo "Done!"

echo "Bulding Docker Image for PySparkling(Python) ..."
cp "$PY_ZIP_FILE" "$WORKDIR"
docker build -t "sparkling-water-python:$VERSION" -f "$WORKDIR/Dockerfile-Python" "$WORKDIR"
echo "Done!"

echo "Bulding Docker Image for RSparkling(R) ..."
cp "$TOPDIR/rsparkling_$VERSION.tar.gz" "$WORKDIR"
docker build -t "sparkling-water-r:$VERSION" -f "$WORKDIR/Dockerfile-R" "$WORKDIR"
echo "Done!"

echo "Cleaning up temporary directories"
rm -rf "$WORKDIR"

echo "All done! You can find your images by running: docker images"
