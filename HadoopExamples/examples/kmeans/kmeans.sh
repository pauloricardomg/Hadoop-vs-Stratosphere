#!/bin/bash

### Libs

# Hadoop installation path
HADOOP_DIR=/home/paulo/emdc/hadoop/hadoop-0.20.203.0

# Hadoop vs Stratosphere project path
PROJ_DIR=/home/paulo/workspace/HadoopVsStratosphere/

### K-means parameters

# Points file (input)
POINTS_FILE=/home/paulo/workspace/HadoopVsStratosphere/examples/kmeans/points.txt

# Centers file (output)
FINAL_OUTPUT=centers.txt

NUM_CLUSTERS=4

### Advanced config

TMP_OUTPUT=./output
OUTPUT_PREFIX="part-*"

# Create empty previous centers file
touch prev.txt
# Create next centers file (first N points (sorted))
shuf --random-source=$0 -n $NUM_CLUSTERS $POINTS_FILE | sort -n -k 1 $INITIAL_CENTERS > next.txt

iteration=0
# Iterate while the previous centers are different from the next
diff prev.txt next.txt >/dev/null
while [ $? -eq 1 ]; do
	# Run 1 iteration of map reduce
	$HADOOP_DIR/bin/hadoop -cp $HADOOP_DIR/lib/*:$HADOOP_DIR/*:$PROJ_DIR/bin/ se.kth.emdc.examples.kmeans.KmeansHadoopMR $POINTS_FILE next.txt $TMP_OUTPUT

	# Next centers become previous centers
	mv next.txt prev.txt

	# Write next centers file (from multiple files to a single file)
	for file in `ls output/$OUTPUT_PREFIX`
	do
		while read line
		do
			echo $line | awk '{print $3"\t"$4}' >> tmp.txt
		done < $file
	done

	# Sort next centers
	sort -n -k 1 tmp.txt > next.txt
	rm -rf $TMP_OUTPUT tmp.txt

	echo "Next centers:"
	cat next.txt

	iteration=`expr $iteration + 1`

	# Compares previous centers with next centers
	diff prev.txt next.txt >/dev/null
done

# Removes previous centers and writes output
rm -rf prev.txt
mv next.txt $FINAL_OUTPUT
echo "Kmeans has converged. Cluster centers are in file "$FINAL_OUTPUT" and gnuplot file is kmeans.plt" 

echo "Generating graph.."
echo "plot \"$POINTS_FILE\" using 1:2 title \"Points\", \\" > result.plt
echo "\"$FINAL_OUTPUT\" using 1:2 title \"Clusters\" " >> result.plt
gnuplot result.plt -persist

echo "Iterations: "$iteration
