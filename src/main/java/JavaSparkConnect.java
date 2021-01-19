import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;


public final class JavaSparkConnect {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .appName("JavaSparkPi")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());


//        List<TableModel> rowList = new ArrayList<>();
//
//        TableModel data1 = new TableModel();
//        data1.setT1("im the first T1");
//        data1.setT2("im the first T2");
//        data1.setT3(1);
//
//        rowList.add(data1);
//
//        TableModel data2 = new TableModel();
//        data2.setT1("im the second T1");
//        data2.setT2("im the second T2");
//        data2.setT3(2);
//        rowList.add(data2);
//
//        Dataset saveData = spark.createDataFrame(rowList, TableModel.class);
//
//        saveData.write().format("Hive")
//                .saveAsTable("test.newtable1");

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