#!/bin/bash
set -e
cd "$(dirname "${BASH_SOURCE[0]}")"

CLUSTER_MODE=${1:-internal}
SPARK_WORKERS_COUNT=4
H2O_CLUSTER_SIZE=4
DRIVER_MEMORY=4g
EXECUTOR_MEMORY=4g
H2O_CLUSTER_SIZE=0
H2O_START_MODE=auto

../gradlew :sparkling-water-benchmarks:build :sparkling-water-benchmarks:substituteScripts
source ./build/local/variables.sh

WORKING_DIR_HOST="/tmp/working-dir"
mkdir -p "$WORKING_DIR_HOST"
chmod 777 "$WORKING_DIR_HOST"

if [ ! -f ./build/data/airlines-1m.csv ]; then
  aws s3 cp s3://ai.h2o.sparkling/benchmarks/data/airlines-1m.csv ./build/data
  aws s3 cp s3://ai.h2o.sparkling/benchmarks/data/airlines-10m.csv ./build/data
  aws s3 cp s3://ai.h2o.sparkling/benchmarks/data/springleaf_train_4.csv ./build/data
fi

if [ "$CLUSTER_MODE" = "external" ]; then
  H2O_CLUSTER_SIZE=$SPARK_WORKERS_COUNT
  H2O_START_MODE=manual
fi

docker-compose --file ./build/local/docker-compose.yml up --detach --scale spark-worker=${SPARK_WORKERS_COUNT} --scale h2o=${H2O_CLUSTER_SIZE} --force-recreate
DOCKER_SW_DIR="/tmp/sparkling-water"

DOCKER_RESULTS_DIR="/tmp/benchmark-results"

docker-compose --file ./build/local/docker-compose.yml exec spark-master mkdir -p $DOCKER_RESULTS_DIR
docker-compose --file ./build/local/docker-compose.yml exec spark-master spark-submit \
  --class ai.h2o.sparkling.benchmarks.Runner \
  --jars "$DOCKER_SW_DIR/assembly/build/libs/${SW_PACKAGE_FILE_NAME}" \
  --master "spark://spark-master:7077" \
  --files "$DOCKER_SW_DIR/benchmarks/build/resources/main/datasetsLocal.json" \
  --driver-memory "${DRIVER_MEMORY}" \
  --executor-memory "${EXECUTOR_MEMORY}" \
  --deploy-mode client \
  --num-executors ${H2O_CLUSTER_SIZE} \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.ext.h2o.backend.cluster.mode=${CLUSTER_MODE}" \
  --conf "spark.ext.h2o.external.cluster.size=${H2O_CLUSTER_SIZE}" \
  --conf "spark.ext.h2o.external.memory=${EXECUTOR_MEMORY}" \
  --conf "spark.ext.h2o.external.start.mode=${H2O_START_MODE}" \
  --conf spark.ext.h2o.cloud.representative=local_h2o_1:54321 \
  --conf spark.ext.h2o.cloud.name=root \
  --conf "spark.ext.h2o.external.extra.jars=$DOCKER_SW_DIR/benchmarks/build/libs/${SW_BENCHMARKS_FILE_NAME}" \
  "$DOCKER_SW_DIR/benchmarks/build/libs/${SW_BENCHMARKS_FILE_NAME}" \
  -s datasetsLocal.json \
  -o "$DOCKER_RESULTS_DIR" \
  -w "file:///tmp/working-dir/"

SPARK_MASTER_DOCKER_ID=$(docker-compose --file ./build/local/docker-compose.yml ps -q spark-master)

docker cp "$SPARK_MASTER_DOCKER_ID":$DOCKER_RESULTS_DIR ./build/benchmark-results/

docker-compose --file ./build/local/docker-compose.yml down
rm -rf "$WORKING_DIR_HOST"
