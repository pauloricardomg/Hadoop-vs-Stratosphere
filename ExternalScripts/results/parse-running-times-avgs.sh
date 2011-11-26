#!/bin/bash

E_BADARGS=65

#Check arguments
if [ $# -ne 2 ]; then
	  echo "Usage: `basename $0` {inputFolder} {outputFile}"
	    exit $E_BADARGS
fi

INPUT_FOLDER=$1
OUTPUT_FILE=$2

CSV2GNUPLOT=./csv2gnuplot.sh

REDUCERS=6
BLOCK_SIZE=64

echo -n > $OUTPUT_FILE
for size in 64 384 768 1536; do
	echo -n $size" " >> $OUTPUT_FILE
	mappers=`echo $size / $BLOCK_SIZE | bc`
	$CSV2GNUPLOT -t -a -i $INPUT_FOLDER"/time_"$size"M_m"$mappers"_r"$REDUCERS >> $OUTPUT_FILE
done	
