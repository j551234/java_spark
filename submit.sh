#!/bin/bash

export HADOOP_USER_NAME=hdfs
export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2/

$SPARK_HOME/bin/spark-submit   \
   --master yarn \
   --class JavaSparkConnect \
   /opt/libs/sparkTest-1.0-SNAPSHOT.jar 80
