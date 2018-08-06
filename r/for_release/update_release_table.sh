#!/usr/bin/env bash

echo $1 $2 $3 $4
R -e "source('for_release/utils.R'); update_release_tables($1, $2, $3, $4); write_release_table('build/r_release_table.csv', $1)"

python for_release/csv2rst.py "build/r_release_table.csv" > build/r_release_table.rst



