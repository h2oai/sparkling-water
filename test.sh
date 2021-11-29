#!/bin/bash

for file in $(find . -name "build.gradle"); do
  echo $file | cut -c 3-
done