#!/usr/bin/env bash

curl -X POST -d @nexus_description.xml -u $1:$2 -H Content-Type:application/xml -v https://oss.sonatype.org/service/local/staging/profiles/3ce9ff9ad792da/start \
| grep stagedRepositoryId | cut -d ">" -f2 | cut -d"<" -f1
