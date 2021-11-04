#!/bin/bash

export HADOOP_USER_NAME=root
export SPARK_HOME=/opt/spark

$SPARK_HOME/bin/spark-submit   \
   --master local \
   --deploy-mode client \
   --files /home/james/java_spark/hive-site.xml \
   --class JavaSparkSql \
   --jars $(echo /home/james/java_spark/build/libs/lib/*.jar | tr ' ' ',') \
   /home/james/java_spark/build/libs/java_spark-1.0-SNAPSHOT.jar 80
