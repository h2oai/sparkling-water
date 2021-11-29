#!/bin/bash

for file in $(find . -name "build.gradle"); do
  echo "$file was added"
done