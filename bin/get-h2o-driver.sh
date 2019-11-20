#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd "$(dirname "$0")/.."; pwd)

source "$TOPDIR/bin/sparkling-env.sh"

if [ ! $# -eq 1 ]; then
cat <<EOF
Download H2O driver for external H2O backend mode of Sparkling Water.

 $0 <HADOOP_VERSION>

 Parameters:
    HADOOP_VERSION - Hadoop version (e.g., hdp2.1) or "standalone" - see list below

 Hadoop distributions supported by H2O:
    ${AVAILABLE_H2O_DRIVERS}

EOF
exit
fi

if ! [[ " ${AVAILABLE_H2O_DRIVERS} " =~ " ${1} " ]]; then
cat <<EOF

 ${1} is not supported Hadoop distribution.

 Hadoop distributions supported by H2O:
    ${AVAILABLE_H2O_DRIVERS}

EOF
exit
fi

if [ "$1" == "standalone" ]; then
    hadoop_version=""
else
    hadoop_version="-${1}"
fi
DRIVER_VERSION="$H2O_VERSION.$H2O_BUILD${hadoop_version}"
h2odriver_dist_url="https://h2o-release.s3.amazonaws.com/h2o/rel-${H2O_NAME}/${H2O_BUILD}/h2o-${DRIVER_VERSION}.zip"

output_file="h2odriver-${DRIVER_VERSION}"
WORKDIR=$(mktemp -d)
output_zip_file="$WORKDIR/${output_file}.zip"

echo
echo "Getting H2O driver distribution from ${h2odriver_dist_url} ..."
curl -f -o "$output_zip_file" --progress-bar "$h2odriver_dist_url" && echo && echo "H2O driver distribution saved into: ${output_zip_file}" || echo "File not found: $h2odriver_dist_url"

echo "Unzipping H2O driver distribution and extracting H2O driver jar ..."
unzip -q "${output_zip_file}" -d "$WORKDIR"
if [ "$1" == "standalone" ]; then
    cp "$WORKDIR/h2o-${DRIVER_VERSION}/h2o.jar" "${output_file}.jar"
else
    cp "$WORKDIR/h2o-${DRIVER_VERSION}/h2odriver.jar" "${output_file}.jar"
fi

echo "Removing temporary directory ${WORKDIR} ..."
rm -rf "$WORKDIR"

echo "H2O driver is available at $(pwd)/${output_file}.jar"
