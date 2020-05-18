#!/usr/bin/env bash

echo "Create Sparkling Water Distribution"
./gradlew dist

echo "Build Docker Images"

./dist/buid/zip/sparkling-water-SUBST_SW_VERSION/bin/build-kubernetes-images scala
./dist/buid/zip/sparkling-water-SUBST_SW_VERSION/bin/build-kubernetes-images r
./dist/buid/zip/sparkling-water-SUBST_SW_VERSION/bin/build-kubernetes-images python


