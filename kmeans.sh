#!/bin/bash

HADOOP_DIR=/home/paulo/emdc/hadoop/hadoop-0.20.203.0
PROJ_DIR=/home/paulo/workspace/HadoopVsStratosphere/

$HADOOP_DIR/bin/hadoop -cp $HADOOP_DIR/lib/*:$HADOOP_DIR/*:$PROJ_DIR/bin/ se.kth.emdc.examples.kmeans.KmeansHadoopMR $@
