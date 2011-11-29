#!/bin/bash

### Libs

# Stratosphere installation path
STRATOSPHERE_DIR=/home/paulo/emdc/stratosphere/stratosphere-0.1.1/

### K-means parameters
#Jar file
KMEANS_JAR=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/lib/StratKmeansMR.jar

# Points file (input)
POINTS_FILE=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/points

# Centers file (output)
FINAL_OUTPUT=centers.txt

# Gnu plot output (optional)
GRAPH_OUTPUT=result.plt

NUM_CLUSTERS=50

### Advanced config

PREV=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/prev.txt
NEXT=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/next.txt
TMP=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/tmp.txt
TMP_OUTPUT=/home/paulo/workspace/HadoopVsStratosphere/ExternalScripts/kmeans/output
OUTPUT_PREFIX="*"

# Create empty previous centers file
touch $PREV
# Create next centers file (first N points (sorted))
shuf --random-source=$0 -n $NUM_CLUSTERS $POINTS_FILE | sort -n -k 1 $INITIAL_CENTERS > $NEXT
#cat $NEXT

TOTAL_TIME=0
ITERATION=0

# Iterate while the previous centers are different from the next
diff $PREV $NEXT >/dev/null
while [ $? -eq 1 ]; do
	START=$SECONDS
	
	# Run 1 iteration of map reduce
	$STRATOSPHERE_DIR/bin/pact-client.sh run -v -w -j $KMEANS_JAR -a file://$POINTS_FILE $NEXT file://$TMP_OUTPUT     
	
	# Calculate elapsed time
	ELAPSED=`expr $SECONDS - $START`
	TOTAL_TIME=`expr $TOTAL_TIME + $ELAPSED`

	# Next centers become previous centers
	mv $NEXT $PREV

	cat $TMP_OUTPUT > $TMP

	# Write next centers file (from multiple files to a single file)
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
	#rm -rf $TMP
	#mv $TMP_OUTPUT $TMP_OUTPUT"-"$ITERATION

	echo "Next centers:"
	cat $NEXT

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
