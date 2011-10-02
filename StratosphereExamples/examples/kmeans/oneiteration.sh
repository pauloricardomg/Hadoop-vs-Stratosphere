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

TOTAL_TIME=0
ITERATION=0

# Iterate while the previous centers are different from the next
$STRATOSPHERE_DIR/bin/pact-client.sh run -v -j $KMEANS_JAR -a 4 file://$POINTS_FILE file://$NEXT file://$TMP_OUTPUT   
	
