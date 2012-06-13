#!/bin/sh

SQLITE_FILE=$1

for t in energy perf spin hit
do
    for r in 0.7 0.5 0.3
    do
        for c in 0.01 0.05 0.1
        do
            ./make_dataframe.sh -t $t -r $r -c $c -m 4 -f $SQLITE_FILE > ${t}_rd${r}_rc${c}.csv
        done
    done
done
