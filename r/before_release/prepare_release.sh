#!/usr/bin/env bash

R -f release_table.R
if [ "$1" = "release" ]
then
  R -f update_package_version.R
  R CMD BUILD ../../rsparkling
fi