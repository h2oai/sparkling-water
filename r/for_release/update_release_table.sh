#!/usr/bin/env bash


R -e "source('for_release/utils.R'); write_release_table('build/release_table.csv')"

python for_release/csv2rst.py "build/release_table.csv" > release_table.rst



