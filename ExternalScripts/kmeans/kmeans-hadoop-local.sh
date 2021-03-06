#!/bin/bash

### Libs

# Hadoop installation path
HADOOP_DIR=/home/paulo/emdc/hadoop/hadoop-0.20.203.0/

# Hadoop Examples project path
PROJ_DIR=/home/paulo/workspace/HadoopVsStratosphere/

### K-means parameters

# Points file (input)
POINTS_FILE=/home/paulo/workspace/HadoopVsStratosphere/HadoopExamples/examples/kmeans/points

# Centers file (output)
FINAL_OUTPUT=final-output

# Gnu plot output (optional)
GRAPH_OUTPUT=result.plt

NUM_CLUSTERS=50

### Advanced config

PREV=prev.txt
NEXT=next.txt
TMP=tmp.txt
TMP_OUTPUT=./output
OUTPUT_PREFIX="part-*"

# Create empty previous centers file
touch $PREV
# Create next centers file (first N points (sorted))
shuf --random-source=$0 -n $NUM_CLUSTERS $POINTS_FILE | sort -n -k 1 > $NEXT

TOTAL_TIME=0
ITERATION=0
INITIAL_TIME=$SECONDS

# Iterate while the previous centers are different from the next
diff $PREV $NEXT >/dev/null
while [ $? -eq 1 ]; do
	START=$SECONDS
	
	# Run 1 iteration of map reduce
	$HADOOP_DIR/bin/hadoop -cp $HADOOP_DIR/lib/*:$HADOOP_DIR/*:$PROJ_DIR/HadoopExamples/bin/:$PROJ_DIR/Common/bin/ se.kth.emdc.examples.kmeans.hadoop.Kmeans $POINTS_FILE $NEXT $TMP_OUTPUT
	
	# Calculate elapsed time
	ELAPSED=`expr $SECONDS - $START`
	TOTAL_TIME=`expr $TOTAL_TIME + $ELAPSED`

	# Next centers become previous centers
	mv $NEXT $PREV

	# Write next centers file (from multiple files to a single file)
	cat `ls output/$OUTPUT_PREFIX` > $TMP
	#for file in `ls output/$OUTPUT_PREFIX`
	#do
	#	while read line
	#	do
	#		echo $line | awk '{print $3"\t"$4}' >> $TMP
	#	done < $file
	#done

	# Sort next centers
	sort -n -k 1 $TMP > $NEXT
	rm -rf $TMP_OUTPUT $TMP

	echo "Next centers:"
	cat next.txt

	ITERATION=`expr $ITERATION + 1`

	# Compares previous centers with next centers
	diff $PREV $NEXT >/dev/null
done

GLOBAL_TIME=`expr $SECONDS - $INITIAL_TIME`

# Removes previous centers and writes output
rm -rf $PREV
mv $NEXT $FINAL_OUTPUT
echo "Kmeans has converged. Cluster centers are in file "$FINAL_OUTPUT" and gnuplot file is "$GRAPH_OUTPUT 

echo "Generating graph.."
echo "plot \"$POINTS_FILE\" using 1:2 title \"Points\", \\" > $GRAPH_OUTPUT
echo "\"$FINAL_OUTPUT\" using 1:2 title \"Clusters\" " >> $GRAPH_OUTPUT
gnuplot $GRAPH_OUTPUT -persist

echo "Iterations: "$ITERATION
echo "Hadoop execution time: "$TOTAL_TIME"s"
echo "Total execution time: "$GLOBAL_TIME"s"
