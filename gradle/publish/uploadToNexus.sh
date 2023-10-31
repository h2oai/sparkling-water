#!/usr/bin/env bash

name=$1
password=$2
repodir=$3
stagingId=$4

find $repodir -type f | while read f; do
    suffix=$(echo $f | sed "s%^$repodir/%%")
    echo "x${stagingId}x"
    curl -v -u $name:$password -H "Content-type: application/x-rpm" --upload-file $f https://oss.sonatype.org/service/local/staging/deployByRepositoryId/${stagingId}/${suffix} 2>> out.txt
done
