#!/usr/bin/env bash

# Current dir
TOPDIR=$(cd "$(dirname "$0")/.."; pwd)

source "$TOPDIR/bin/sparkling-env.sh"

if [ ! $# -eq 1 ]; then
cat <<EOF
Download extended H2O driver for Kluster mode.

 $0 <HADOOP_VERSION>
  
 Parameters:
    HADOOP_VERSION - Hadoop version (e.g., hdp2.1) or "standalone" - see list below
    
 Hadoop distributions supported by H2O:
    ${AVAILABLE_H2O_DRIVERS}

EOF
exit
fi

if [ "$1" == "standalone" ]; then
    hadoop_version=""
else
    hadoop_version="${1}-"
fi

h2odriver_file="h2odriver-$H2O_VERSION.$H2O_BUILD-${hadoop_version}extended.jar"
h2odriver_url="${S3_RELEASE_BUCKET}/rel-${MAJOR_VERSION}/${PATCH_VERSION}/extended/${h2odriver_file}"
output_file="h2odriver-${hadoop_version}extended.jar"

echo "Getting h2odriver from ${output_file}..."
curl -f -o "$output_file" --progress-bar "$h2odriver_url" && echo "H2O driver saved as ${output_file}" || echo "File not found: $h2odriver_url"

