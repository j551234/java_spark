# java_spark
java 使用spark程式範例

## usage
* 用gradle build出jar
* 並使用submit.sh 把job submit到spark的環境上執行
* 連線hive需要hive-site.xml設定檔
* 若要使用yarn的需給予對應的hadoop連線資訊
* 要在環境變數裡新增hadoop相關的系統變數


# submit 腳本
需指定 spark
```shell=
#!/bin/bash

export HADOOP_USER_NAME=hdfs
export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2/

$SPARK_HOME/bin/spark-submit   \
   --master yarn \
   # 指定main class
   --class JavaSparkConnect \
   # 指定hive連線資訊檔案
   --files /home/james/java_spark/hive-site.xml \
   # 指定dependency jar 包
   --jars $(echo /opt/libs/lib/*.jar | tr ' ' ',')
   # 指定要執行的程式jar包
   /opt/libs/sparkTest-1.0-SNAPSHOT.jar 80
   
```  

# Intellij ssh submit spark job

在run的設定中可新增ssh submit

需安裝Big Data tool
https://plugins.jetbrains.com/plugin/12494-big-data-tools
設定使用ssh remote submit
https://www.jetbrains.com/help/idea/big-data-tools-spark-submit.html#spark-submit
需指定ssh連線資訊資訊
設定使用sparkHome
指定上傳jar檔位置
並指定mainClass
在BeforLaunch前新增build的步驟
![](https://i.imgur.com/iMHZpTH.png)



# api document
https://sparkbyexamples.com/spark/spark-drop-column-from-dataframe-dataset/
