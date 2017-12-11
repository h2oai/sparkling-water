#!/bin/bash
n=0
while read l; do
    echo $l > "../data/kuba/input/${n}.csv"
    n=$((n+1))
    sleep 3
    echo "Streaming $n ..."
done < ../data/kuba/AmazonReviews_Predictions.csv
