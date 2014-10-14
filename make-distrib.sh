#!/usr/bin/env bash

cat > demofiles.list <<EOF
sparkling-water/bin/launch-spark-cloud.sh
sparkling-water/bin/run-example.sh
sparkling-water/bin/sparkling-shell
sparkling-water/examples/build/libs/sparkling-water-examples-0.1.6-SNAPSHOT-all.jar
sparkling-water/examples/README.md
sparkling-water/examples/scripts/dlDemo.script
sparkling-water/examples/smalldata/allyears2k_headers.csv.gz
sparkling-water/examples/smalldata/Chicago_Ohare_2010_2013.csv
sparkling-water/examples/smalldata/Chicago_Ohare_International_Airport.csv
sparkling-water/examples/smalldata/Chicago_Ohare_International_Airport_2010_2013.csv
sparkling-water/examples/smalldata/prostate.csv
sparkling-water/LICENSE
sparkling-water/README.md
EOF

ZIPNAME="sparkling-water.zip" 
[ -f "$ZIPNAME" ] && rm $ZIPNAME

(
  cd ../ 
  cat sparkling-water/demofiles.list | zip -@ "sparkling-water/$ZIPNAME"
)

