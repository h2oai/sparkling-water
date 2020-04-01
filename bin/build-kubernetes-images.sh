#!/usr/bin/env bash

if [[ "$#" -ne 1 ]] || [[ "$1" != "scala" && "$1" != "python" && "$1" != "r" ]]; then
  echo "This script expects exactly one argument which specifies for which client the image should be build."
  echo "The possible values are: scala, r, python"
  exit -1
fi


# Current dir
TOPDIR=$(cd "$(dirname "$0")/.." || exit; pwd)

source "$TOPDIR/bin/sparkling-env.sh"

# Verify there is Spark installation
checkSparkHome
# Verify if correct Spark version is used
checkSparkVersion

( cd "$SPARK_HOME" && ./bin/docker-image-tool.sh -t "$INSTALLED_SPARK_FULL_VERSION" build )

echo "Creating Working Directory"
WORKDIR=$(mktemp -d)
echo "Working directory created: $WORKDIR"

K8DIR="$TOPDIR/kubernetes"

if [ "$1" = "scala" ]; then
  cp "$K8DIR/Dockerfile-Scala" "$WORKDIR"
  echo "Building Docker Image for Sparkling Water(Scala) ..."
  cp "$FAT_JAR_FILE" "$WORKDIR"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-scala:$VERSION" -f "$WORKDIR/Dockerfile-Scala" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "python" ]; then
  cp "$K8DIR/Dockerfile-Python" "$WORKDIR"
  echo "Building Docker Image for PySparkling(Python) ..."
  cp "$PY_ZIP_FILE" "$WORKDIR"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-python:$VERSION" -f "$WORKDIR/Dockerfile-Python" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "r" ]; then
  cp "$K8DIR/Dockerfile-R" "$WORKDIR"
  echo "Building Docker Image for RSparkling(R) ..."
  cp "$TOPDIR/rsparkling_$VERSION.tar.gz" "$WORKDIR"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-r:$VERSION" -f "$WORKDIR/Dockerfile-R" "$WORKDIR"
  echo "Done!"
fi

echo "Cleaning up temporary directories"
rm -rf "$WORKDIR"

echo "All done! You can find your images by running: docker images"