#!/bin/bash

installPlugin() {
  if [ -f "${PLUGIN_DIR}/${1}.hpi" -o -f "${PLUGIN_DIR}/${1}.jpi" ]; then
    if [ "$2" == "1" ]; then
      return 1
    fi
    echo "Skipped: $1 (already installed)"
    return 0
  else
    echo "Installing: $1"
    curl -L --silent --output "${PLUGIN_DIR}/${1}.hpi" "https://updates.jenkins-ci.org/latest/${1}.hpi"
    return 0
  fi
}

PLUGIN_DIR=/var/lib/jenkins/plugins
FILE_OWNER=jenkins.jenkins

mkdir -p $PLUGIN_DIR

for plugin in "$@"
do
    installPlugin "$plugin"
done

changed=1
maxloops=100

while [ "$changed"  == "1" ]; do
  echo "Check for missing dependecies ..."
  if  [ $maxloops -lt 1 ] ; then
    echo "Max loop count reached - probably a bug in this script: $0"
    exit 1
  fi
  ((maxloops--))
  changed=0
  for f in "${PLUGIN_DIR}"/*.hpi ; do
    deps=$( unzip -p "${f}" META-INF/MANIFEST.MF | tr -d '\r' | sed -e ':a;N;$!ba;s/\n //g' | grep -e "^Plugin-Dependencies: " | awk '{ print $2 }' | tr ',' '\n' | awk -F ':' '{ print $1 }' | tr '\n' ' ' )
    for plugin in $deps; do
      installPlugin "$plugin" 1 && changed=1
    done
  done
done

echo "Fixing permissions"
chown ${FILE_OWNER} ${PLUGIN_DIR} -R
echo "Plugins Installed"
