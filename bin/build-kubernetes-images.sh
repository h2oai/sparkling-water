#!/usr/bin/env bash

if [[ "$#" -ne 1 ]] || [[ "$1" != "scala" && "$1" != "python" && "$1" != "r"  && "$1" != "external-backend" ]]; then
  echo "This script expects exactly one argument which specifies type of image to be build."
  echo "The possible values are: scala, r, python, external-backend"
  exit -1
fi


# Current dir
TOPDIR=$(cd "$(dirname "$0")/.." || exit; pwd)

source "$TOPDIR/bin/sparkling-env.sh"

# Verify there is Spark installation
checkSparkHome
# Verify if correct Spark version is used
checkSparkVersion

echo "Creating Working Directory"
WORKDIR=$(mktemp -d)
echo "Working directory created: $WORKDIR"
K8DIR="$TOPDIR/kubernetes"

if [ "$1" = "external-backend" ]; then
  cp "$K8DIR/Dockerfile-External-backend" "$WORKDIR"
  echo "Building Docker Image for External Backend ..."
  cp "$TOPDIR/jars/sparkling-water-assembly-extensions_$SCALA_VERSION-$VERSION-all.jar" "$WORKDIR"
  # Enable build Kubernetes images for nightlies. We build nightlies against specific H2O branches and in that
  # case, the name of H2O is always bleeding_edge
  if [ "${H2O_NAME}" = "bleeding_edge" ]; then
      cp "$H2O_HOME/build/h2o.jar" "$WORKDIR/h2o.jar"
  else
    path=$($TOPDIR/bin/get-h2o-driver.sh standalone)
    cp "$path" "$WORKDIR/h2o.jar"
  fi
  docker build -t "sparkling-water-external-backend:$VERSION" -f "$WORKDIR/Dockerfile-External-backend" "$WORKDIR"
  echo "Done!"
  exit 0
fi

( cd "$SPARK_HOME" && ./bin/docker-image-tool.sh -t "$INSTALLED_SPARK_FULL_VERSION" build )

if [ "$1" = "scala" ]; then
  cp "$K8DIR/Dockerfile-Scala" "$WORKDIR"
  echo "Building Docker Image for Sparkling Water(Scala) ..."
  cp "$FAT_JAR_FILE" "$WORKDIR"
  cp -R "$TOPDIR/kubernetes/scala/" "$WORKDIR/scala"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-scala:$VERSION" -f "$WORKDIR/Dockerfile-Scala" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "python" ]; then
  cp "$K8DIR/Dockerfile-Python" "$WORKDIR"
  echo "Building Docker Image for PySparkling(Python) ..."
  cp "$PY_ZIP_FILE" "$WORKDIR"
  cp -R "$TOPDIR/kubernetes/python/" "$WORKDIR/python"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-python:$VERSION" -f "$WORKDIR/Dockerfile-Python" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "r" ]; then
  cp "$K8DIR/Dockerfile-R" "$WORKDIR"
  echo "Building Docker Image for RSparkling(R) ..."
  cp "$TOPDIR/rsparkling_$VERSION.tar.gz" "$WORKDIR"
  # Enable build Kubernetes images for nightlies. We build nightlies against specific H2O branches and in that
  # case, the name of H2O is always bleeding_edge
  echo "${H2O_NAME}"
  echo "http://h2o-release.s3.amazonaws.com/h2o/rel-${H2O_NAME}/${H2O_BUILD}/R/src/contrib/h2o_${H2O_VERSION}.${H2O_BUILD}.tar.gz"
  if [ "${H2O_NAME}" = "bleeding_edge" ]; then
    cp "$H2O_HOME/h2o-r/h2o_${H2O_VERSION}.99999.tar.gz" "$WORKDIR/h2o.tar.gz"
  else
    curl "http://h2o-release.s3.amazonaws.com/h2o/rel-${H2O_NAME}/${H2O_BUILD}/R/src/contrib/h2o_${H2O_VERSION}.${H2O_BUILD}.tar.gz" --output "$WORKDIR/h2o.tar.gz"
  fi
  cp "$FAT_JAR_FILE" "$WORKDIR"
  cp -R "$TOPDIR/kubernetes/r/" "$WORKDIR/r"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-r:$VERSION" -f "$WORKDIR/Dockerfile-R" "$WORKDIR"
  echo "Done!"
fi

echo "Cleaning up temporary directories"
rm -rf "$WORKDIR"

echo "All done! You can find your images by running: docker images"
