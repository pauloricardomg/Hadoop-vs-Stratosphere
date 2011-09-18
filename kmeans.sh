#!/bin/bash

HADOOP_DIR=/home/paulo/emdc/hadoop/hadoop-0.20.203.0
PROJ_DIR=/home/paulo/workspace/HadoopVsStratosphere/

POINTS_FILE=/home/paulo/workspace/HadoopVsStratosphere/src/se/kth/emdc/examples/kmeans/myPoints.txt

NUM_CLUSTERS=2

TMP_OUTPUT=./output
OUTPUT_PREFIX="part-*"

touch prev.txt
head -n $NUM_CLUSTERS $POINTS_FILE | sort -n -k 1 $INITIAL_CENTERS > next.txt

diff prev.txt next.txt >/dev/null
while [ $? -eq 1 ]; do
	# Run 1 iteration of map reduce
	$HADOOP_DIR/bin/hadoop -cp $HADOOP_DIR/lib/*:$HADOOP_DIR/*:$PROJ_DIR/bin/ se.kth.emdc.examples.kmeans.KmeansHadoopMR $POINTS_FILE next.txt $TMP_OUTPUT

	mv next.txt prev.txt

	for file in `ls output/$OUTPUT_PREFIX`
	do
		while read line
		do
			echo $line | awk '{print $3"\t"$4}' >> tmp.txt
		done < $file
	done

	sort -n -k 1 tmp.txt > next.txt
	rm -rf $TMP_OUTPUT tmp.txt

	echo "Next centers:"
	cat next.txt

	diff prev.txt next.txt >/dev/null
done

rm -rf prev.txt
mv next.txt centers.txt
echo "Kmeans has converged. Cluster centers are in file centers.txt"
