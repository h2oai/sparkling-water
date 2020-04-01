#!/usr/bin/env bash

curl -X POST -d "
<promoteRequest>
    <data>
        <stagedRepositoryId>$3</stagedRepositoryId>
        <description>Sparkling Water Release</description>
    </data>
</promoteRequest>" -u $1:$2 -H Content-Type:application/xml -v https://oss.sonatype.org/service/local/staging/profiles/3ce9ff9ad792da/finish
