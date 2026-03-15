#!/usr/bin/env bash

if [[ "$#" -lt 1 ]] || [[ "$1" != "scala" && "$1" != "python" && "$1" != "r" && "$1" != "external-backend" && "$1" != "hardened" ]]; then
  echo "This script expects at least one argument which specifies type of image to be built."
  echo "The possible values are: scala, r, python, external-backend, hardened"
  exit 1
fi

set -e # fail on error

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
K8DISTDIR="$K8DIR/build/dist"

resolve_k8_path() {
  local rel_path="$1"
  if [ -e "$K8DIR/$rel_path" ]; then
    echo "$K8DIR/$rel_path"
  elif [ -e "$K8DISTDIR/$rel_path" ]; then
    echo "$K8DISTDIR/$rel_path"
  else
    # Fall back to the original location to surface a clear cp error.
    echo "$K8DIR/$rel_path"
  fi
}

if [ "$1" = "hardened" ]; then
  echo "Building Hardened Docker Image for Sparkling Water ..."
  DOCKERFILE="$K8DIR/Dockerfile-Hardened"
  if [ ! -f "$DOCKERFILE" ]; then
    echo "ERROR: $DOCKERFILE not found"
    exit 1
  fi

  mkdir -p "$WORKDIR/spark-jars" "$WORKDIR/spark-python" "$WORKDIR/spark-bin" \
           "$WORKDIR/spark-sbin" "$WORKDIR/spark-entrypoint"

  # Copy Spark JARs
  cp "$SPARK_HOME"/jars/*.jar "$WORKDIR/spark-jars/"

  # Remove fabric8 6.x so the assembly's 7.x is used
  rm -fv "$WORKDIR"/spark-jars/kubernetes-client*.jar \
         "$WORKDIR"/spark-jars/kubernetes-model*.jar \
         "$WORKDIR"/spark-jars/volcano*.jar 2>/dev/null || true

  # Download hadoop-aws + AWS SDK bundle if missing
  HADOOP_VERSION="${HADOOP_VERSION:-3.4.2}"
  if [ -z "$(ls "$WORKDIR"/spark-jars/hadoop-aws-*.jar 2>/dev/null)" ]; then
    curl -fL -o "$WORKDIR/spark-jars/hadoop-aws-${HADOOP_VERSION}.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar"
  fi
  if [ -z "$(ls "$WORKDIR"/spark-jars/aws-java-sdk-bundle-*.jar 2>/dev/null)" ]; then
    curl -fL -o "$WORKDIR/spark-jars/aws-java-sdk-bundle-1.12.797.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.797/aws-java-sdk-bundle-1.12.797.jar"
  fi

  # Spark python / bin / sbin / entrypoint
  cp -r "$SPARK_HOME"/python/* "$WORKDIR/spark-python/" 2>/dev/null || true
  cp -r "$SPARK_HOME"/bin/* "$WORKDIR/spark-bin/"
  chmod +x "$WORKDIR"/spark-bin/* || true
  cp -r "$SPARK_HOME"/sbin/* "$WORKDIR/spark-sbin/" 2>/dev/null || true

  if [ -f "$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh" ]; then
    cp "$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh" "$WORKDIR/spark-entrypoint/"
  elif [ -f "$SPARK_HOME/bin/entrypoint.sh" ]; then
    cp "$SPARK_HOME/bin/entrypoint.sh" "$WORKDIR/spark-entrypoint/"
  else
    echo "ERROR: entrypoint.sh not found in SPARK_HOME"
    exit 1
  fi
  chmod +x "$WORKDIR/spark-entrypoint/entrypoint.sh"

  # Copy assembly JAR
  cp "$FAT_JAR_FILE" "$WORKDIR/sparkling-water-assembly.jar"

  # Copy PySparkling zip
  cp "$PY_ZIP_FILE" "$WORKDIR/"

  docker build --pull --progress=plain \
    --build-arg HADOOP_VERSION="${HADOOP_VERSION}" \
    --build-arg PYSPARK_VERSION="${INSTALLED_SPARK_FULL_VERSION}" \
    -t "sparkling-water-hardened:$VERSION" \
    -f "$DOCKERFILE" "$WORKDIR"

  echo "Done! Image: sparkling-water-hardened:$VERSION"
  rm -rf "$WORKDIR"
  exit 0
fi

if [ "$1" = "external-backend" ]; then
  cp "$(resolve_k8_path Dockerfile-External-backend)" "$WORKDIR"
  echo "Building Docker Image for External Backend ..."
  cp "$TOPDIR/jars/sparkling-water-assembly-extensions_$SCALA_VERSION-$VERSION-all.jar" "$WORKDIR"
  # Enable build Kubernetes images for nightlies. We build nightlies against specific H2O branches and in that
  # case, the name of H2O is always bleeding_edge
  if [ "${H2O_NAME}" = "bleeding_edge" ]; then
      cp "$H2O_HOME/build/h2o.jar" "$WORKDIR/h2o.jar"
  fi
  docker build -t "sparkling-water-external-backend:$VERSION" -f "$WORKDIR/Dockerfile-External-backend" "$WORKDIR"
  echo "Done!"
  exit 0
fi

if [ "$1" = "python" ]; then
  (cd "$SPARK_HOME" && \
   ./bin/docker-image-tool.sh -t "$INSTALLED_SPARK_FULL_VERSION" \
     -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile \
     -b java_image_tag=17-jammy build)
elif [ "$1" = "r" ]; then
  (cd "$SPARK_HOME" && \
   TMP_SPARK_R_DOCKERFILE=$(mktemp) && \
   sed  "s/apt-key adv --keyserver keys.gnupg.net --recv-key 'E19F5F87128899B192B1A2C2AD5F960A256A04AF'/apt-key adv --keyserver keyserver.ubuntu.com --recv-key FCAE2A0E115C3D8A/g" ./kubernetes/dockerfiles/spark/bindings/R/Dockerfile >> "$TMP_SPARK_R_DOCKERFILE" && \
   ./bin/docker-image-tool.sh -t "$INSTALLED_SPARK_FULL_VERSION" \
     -R "$TMP_SPARK_R_DOCKERFILE" \
     -b java_image_tag=17-jammy build && \
   rm "$TMP_SPARK_R_DOCKERFILE")
else
  (cd "$SPARK_HOME" && \
   RDOCKERFILE=false PYDOCKERFILE=false \
   ./bin/docker-image-tool.sh -t "$INSTALLED_SPARK_FULL_VERSION" \
     -b java_image_tag=17-jammy build)
fi
if [ "$1" = "scala" ]; then
  cp "$(resolve_k8_path Dockerfile-Scala)" "$WORKDIR"
  echo "Building Docker Image for Sparkling Water(Scala) ..."
  cp "$FAT_JAR_FILE" "$WORKDIR"
  cp -R "$(resolve_k8_path scala)/" "$WORKDIR/scala"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-scala:$VERSION" -f "$WORKDIR/Dockerfile-Scala" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "python" ]; then
  cp "$(resolve_k8_path Dockerfile-Python)" "$WORKDIR"
  echo "Building Docker Image for PySparkling(Python) ..."
  cp "$PY_ZIP_FILE" "$WORKDIR"
  cp -R "$(resolve_k8_path python)/" "$WORKDIR/python"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-python:$VERSION" -f "$WORKDIR/Dockerfile-Python" "$WORKDIR"
  echo "Done!"
fi

if [ "$1" = "r" ]; then
  cp "$(resolve_k8_path Dockerfile-R)" "$WORKDIR"
  echo "Building Docker Image for RSparkling(R) ..."
  cp "$TOPDIR/rsparkling_$VERSION.tar.gz" "$WORKDIR"
  # Enable build Kubernetes images for nightlies. We build nightlies against specific H2O branches and in that
  # case, the name of H2O is always bleeding_edge
  if [ "${H2O_NAME}" = "bleeding_edge" ]; then
    cp "$H2O_HOME/h2o-r/h2o_${H2O_VERSION}.99999.tar.gz" "$WORKDIR/h2o.tar.gz"
  else
    curl "http://h2o-release.s3.amazonaws.com/h2o/rel-${H2O_NAME}/${H2O_BUILD}/R/src/contrib/h2o_${H2O_VERSION}.${H2O_BUILD}.tar.gz" --output "$WORKDIR/h2o.tar.gz"
  fi
  cp "$FAT_JAR_FILE" "$WORKDIR"
  cp -R "$(resolve_k8_path r)/" "$WORKDIR/r"
  docker build --build-arg "spark_version=$INSTALLED_SPARK_FULL_VERSION" -t "sparkling-water-r:$VERSION" -f "$WORKDIR/Dockerfile-R" "$WORKDIR"
  echo "Done!"
fi

echo "Cleaning up temporary directories"
rm -rf "$WORKDIR"

echo "All done! You can find your images by running: docker images"
