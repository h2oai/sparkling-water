#!/usr/bin/env bash

R -e "source('for_release/utils.R'); write_release_table('build/r_release_table.csv', $1, $2, $3, $4, $5)"

python for_release/csv2rst.py "build/r_release_table.csv" > build/r_release_table.rst



