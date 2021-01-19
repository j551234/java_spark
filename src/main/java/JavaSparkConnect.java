import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.List;


public final class JavaSparkConnect {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
                .appName("JavaSpark")
                .getOrCreate();

//        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        // example for save
        List<TableModel> rowList = new ArrayList<>();

        TableModel data1 = new TableModel();
        data1.setT1("im the first T1 v");
        data1.setT2("im the first T2 v");
        data1.setT3(1);

        rowList.add(data1);

        TableModel data2 = new TableModel();
        data2.setT1("im the second T1 v");
        data2.setT2("im the second T2 v");
        data2.setT3(2);
        rowList.add(data2);

        Dataset<Row> saveData = spark.createDataFrame(rowList, TableModel.class);

        saveData.write().format("Hive")
                .mode(SaveMode.Append)
                .saveAsTable("test.table1");

        // example for select
        Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);
        System.out.println("===================new table=================");
        Dataset<TableModel> newTable =
                spark.sql("select * from test.newtable1  ").as(personEncoder);
        newTable.show();
        System.out.println("=================old table===================");
        Dataset<TableModel> testTable =
                spark.sql("select * from test.table1  ").as(personEncoder);
        testTable.show();

        //example for select as
        Dataset<TableModel> joinRow = testTable.join(newTable, (
                        newTable.col("t3").equalTo(testTable.col("t3"))),
                "left")
                .select(newTable.col("t1").as("t1"),
                        testTable.col("t2").as("t2"),
                        newTable.col("t3").as("t3")).as(personEncoder);
        joinRow.show();
        // example for join
        Dataset<Row> showRow = joinRow.join(testTable, joinRow.col("t3").equalTo(testTable.col("t3")));

        showRow.show();

        //example for aggregation function
        Dataset<Row> showTable =
                spark.sql("select * from test.table1  ");

        Dataset<Row> aggTable = showTable.groupBy("t1").agg(
                functions.count("t1").as("count"),
                functions.sum("t3").as("maxt3"),
                functions.current_date().as("currentdate")
        );

        aggTable.show();

        System.out.println("======================join table====================");


        spark.stop();
    }


}