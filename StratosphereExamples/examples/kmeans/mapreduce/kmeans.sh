#!/bin/bash

### Libs

# Stratosphere installation path
STRATOSPHERE_DIR=/home/xzh/Programs/stratosphere-0.1.1

# Hadoop Examples project path
PROJ_DIR=/home/xzh/EclipseWorkSpaces/Hadoop-vs-Stratosphere/StratosphereExamples

### K-means parameters
#Jar file
KMEANS_JAR=$PROJ_DIR/examples/kmeans/KmeansPact.jar

# Points file (input)
POINTS_FILE=$PROJ_DIR/examples/kmeans/points.txt

# Centers file (output)
FINAL_OUTPUT=centers.txt

# Gnu plot output (optional)
GRAPH_OUTPUT=result.plt

NUM_CLUSTERS=4

### Advanced config

PREV=prev.txt
NEXT=next.txt
TMP=tmp.txt
TMP_OUTPUT=/home/xzh/EclipseWorkSpaces/Hadoop-vs-Stratosphere/StratosphereExamples/examples/kmeans/output
OUTPUT_PREFIX="*"

# Create empty previous centers file
touch $PREV
# Create next centers file (first N points (sorted))
shuf --random-source=$0 -n $NUM_CLUSTERS $POINTS_FILE | sort -n -k 1 $INITIAL_CENTERS > $NEXT
cat $NEXT

TOTAL_TIME=0
ITERATION=0

# Iterate while the previous centers are different from the next
diff $PREV $NEXT >/dev/null
while [ $? -eq 1 ]; do
	START=$SECONDS
	
	# Run 1 iteration of map reduce
	$STRATOSPHERE_DIR/bin/pact-client.sh run -v -w -j $KMEANS_JAR -a 1 file://$POINTS_FILE file://$NEXT file://$TMP_OUTPUT     
	
	# Calculate elapsed time
	ELAPSED=`expr $SECONDS - $START`
	TOTAL_TIME=`expr $TOTAL_TIME + $ELAPSED`

	# Next centers become previous centers
	mv $NEXT $PREV

	# Write next centers file (from multiple files to a single file)
	for file in `ls output/$OUTPUT_PREFIX`
	do
		while read line
		do
			echo $line | awk '{print $3"\t"$4}' >> $TMP
		done < $file
	done

	# Sort next centers
	sort -n -k 1 $TMP > $NEXT
	#rm -rf $TMP_OUTPUT $TMP
	rm -rf $TMP
	mv $TMP_OUTPUT $TMP_OUTPUT"-"$ITERATION

	#echo "Next centers:"
	#cat next.txt

	ITERATION=`expr $ITERATION + 1`

	# Compares previous centers with next centers
	diff $PREV $NEXT >/dev/null
done

# Removes previous centers and writes output
rm -rf $PREV
mv $NEXT $FINAL_OUTPUT
echo "Kmeans has converged. Cluster centers are in file "$FINAL_OUTPUT" and gnuplot file is "$GRAPH_OUTPUT 

echo "Generating graph.."
echo "plot \"$POINTS_FILE\" using 1:2 title \"Points\", \\" > $GRAPH_OUTPUT
echo "\"$FINAL_OUTPUT\" using 1:2 title \"Clusters\" " >> $GRAPH_OUTPUT
gnuplot $GRAPH_OUTPUT -persist

echo "Iterations: "$ITERATION
echo "Stratosphere execution time: "$TOTAL_TIME"s"