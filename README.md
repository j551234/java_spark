# sparkTest

# submit 腳本
需指定 spark 版本用2.4
```shell=
#!/bin/bash

export HADOOP_USER_NAME=hdfs
export SPARK_HOME=/opt/cloudera/parcels/SPARK2/lib/spark2/

$SPARK_HOME/bin/spark-submit   \
   --master yarn \
   --class JavaSparkConnect \
   /opt/libs/sparkTest-1.0-SNAPSHOT.jar 80
   
```  

# build.gradle
```groovy=
plugins {
    id 'java'
}


group 'org.example'
version '1.0-SNAPSHOT'

repositories {
    mavenCentral()
//     if use cdh jar need to set cdh repository
//        maven {
//        url 'https://repository.cloudera.com/artifactory/cloudera-repos'
//    }
}

dependencies {
    testImplementation 'org.junit.jupiter:junit-jupiter-api:5.6.0'
    testRuntimeOnly 'org.junit.jupiter:junit-jupiter-engine'
//    implementation(  group: 'org.apache.spark', name: 'spark-sql_2.10', version: '1.6.0-cdh5.13.3')
    compile group: 'org.apache.spark', name: 'spark-sql_2.12', version: '2.4.0'
}


// 清除現有的lib
task clearJar(type: Delete) {
    delete "$buildDir\\libs\\lib"
}

//將所有將依賴的jar包複製到lib裡
task copyJar(type: Copy, dependsOn: 'clearJar') {
    from configurations.compileClasspath
    into "$buildDir\\libs\\lib"
}

jar {
    // 例外所有的jar
    excludes = ["*.jar"]

    //lib資料夾的重新輸出
    dependsOn clearJar
    dependsOn copyJar

    // 將依賴jar路徑輸出到manifest裡面
    manifest {

        attributes "Manifest-Version": 1.0,
                'Main-Class': 'JavaSparkPi',
                'Class-Path': configurations.compileClasspath.files.collect { "lib/$it.name" }.join(' ')
    }
}

test {
    useJUnitPlatform()
}
```


# JAVA example
```java=

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;


public final class JavaSparkConnect {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .appName("JavaSpark")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


        List<TableModel> rowList = new ArrayList<>();

        TableModel data1 = new TableModel();
        data1.setT1("im the first T1");
        data1.setT2("im the first T2");
        data1.setT3(1);

        rowList.add(data1);

        TableModel data2 = new TableModel();
        data2.setT1("im the second T1");
        data2.setT2("im the second T2");
        data2.setT3(2);
        rowList.add(data2);

        Dataset saveData = spark.createDataFrame(rowList, TableModel.class);

        saveData.write().format("Hive")
                .saveAsTable("test.newtable1");

        Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);

        Dataset<TableModel> row = spark.sql("select * from test.newtable1  ").as(personEncoder);

        row.show();

        Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);
        System.out.println("===================new table=================");
        Dataset<TableModel> newTable =
                spark.sql("select * from test.newtable1  ").as(personEncoder);
        newTable.show();
        System.out.println("=================old table===================");
        Dataset<TableModel> testTable =
                spark.sql("select * from test.table1  ").as(personEncoder);
        testTable.show();
        Dataset<Row> joinRow = testTable.join(newTable, (
                        newTable.col("t3").equalTo(testTable.col("t3")).and(testTable.col("t3").gt(1))),
                "inner");
        joinRow.show();


        System.out.println("======================join table====================");


        spark.stop();
    }


}
```

# 用法 
先建立sparksession 
```java=
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .appName("JavaSpark")
                .getOrCreate();
```

在用 sparksql去執行sql指令，
並且可用encoder去轉換取回的Dataset<T>的物件型態
預設回傳型態為Dataset<Row>
```java=
  Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);

        Dataset<TableModel> testTable =
                spark.sql("select * from test.table1  ").as(personEncoder);
```

存入方式使用Dataset格式存入
mode可選擇取代SaveMode
並指定寫入table
```java=
        List<TableModel> rowList = new ArrayList<>();

        TableModel data1 = new TableModel();
        data1.setT1("im the first T1");
        data1.setT2("im the first T2");
        data1.setT3(1000);

        rowList.add(data1);
        Dataset saveData = spark.createDataFrame(rowList, TableModel.class);

        saveData.write().format("Hive")
                .mode(SaveMode.Append)
                .saveAsTable("test.table1");

```
mode如果使用overwrite要在建立sparksession時設定屬性
```java=
   SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
                .appName("JavaSpark")
                .getOrCreate();


        List<TableModel> rowList = new ArrayList<>();

        TableModel data1 = new TableModel();
        data1.setT1("im the first T1");
        data1.setT2("im the first T2");
        data1.setT3(1000);

        rowList.add(data1);

        TableModel data2 = new TableModel();
        data2.setT1("im the second T1");
        data2.setT2("im the second T2");
        data2.setT3(2000);
        rowList.add(data2);

        Dataset saveData = spark.createDataFrame(rowList, TableModel.class);

        saveData.write().format("Hive")
                .mode(SaveMode.Overwrite)
                .saveAsTable("test.table1");
```

若插入使用動態插入partition則需設定hive屬性
```java=
        spark.sql("set hive.exec.dynamic.partition=true");
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict");
```



可以用select 出來的欄位當做物件
但名稱（alias)必須和要放入的物件相同
join的指定條件可用
```java=
        Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);
        System.out.println("===================new table=================");
        Dataset<TableModel> newTable =
                spark.sql("select * from test.newtable1  ").as(personEncoder);
        newTable.show();
        System.out.println("=================old table===================");
        Dataset<TableModel> testTable =
                spark.sql("select * from test.table1  ").as(personEncoder);
        testTable.show();
        System.out.println("=================join table===================");    
        Dataset<TableModel> joinRow = testTable.join(newTable, (
                        newTable.col("t3").equalTo(testTable.col("t3"))),
                "left")
                .select(newTable.col("t1").alias("t1"),
                        testTable.col("t2").alias("t2"),
                        newTable.col("t3").alias("t3")).as(personEncoder);
```
groupby 寫法
agg後面可用fuctions 來使用聚合function
```java=
        Dataset<Row> showTable =
                spark.sql("select * from test.table1  ");

       Dataset<Row> aggTable =showTable.groupBy("t1").agg(
                functions.count("t1").as("count"),
                functions.sum("t3").as("maxt3")
        );
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
