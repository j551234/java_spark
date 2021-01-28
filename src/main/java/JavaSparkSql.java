import org.apache.spark.sql.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public final class JavaSparkSql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
                .appName("JavaSpark")
                .getOrCreate();
        Dataset<Row> tables = spark.sql("show tables");
        tables.show();
        // example for save
        List<TableModel> rowList = new ArrayList<>();

        TableModel data1 = new TableModel();

        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.applyPattern("yyyy-MM-dd HH:mm:ss a");
        Date date = new Date();

        data1.setT1("T1@" + sdf.format(date));
        data1.setT2("T2@" + sdf.format(date));
        data1.setT3(1);
        rowList.add(data1);


        Dataset<Row> saveData = spark.createDataFrame(rowList, TableModel.class);
        // example for save
        saveData.write().format("Hive")
                .mode(SaveMode.Append)
                .saveAsTable("test.table1");

        // example for select as object
        Encoder<TableModel> personEncoder = Encoders.bean(TableModel.class);
        Dataset<TableModel> newTable =
                spark.sql("select * from test.table1  ").select(
                      "t1","t2","t3"
                ).as(personEncoder);
        newTable.show();

        List<TableModel> tableModelList = newTable.collectAsList();
        tableModelList.forEach(s -> System.out.println("this is" + s.getT1()));


        // xx test model will be set by name
        Dataset<Row> showTable =
                spark.sql("select * from test.newtable1  ");

        Encoder<JoinTable> tableModelEncoder = Encoders.bean(JoinTable.class);
        Dataset<JoinTable> aggTable =showTable.groupBy("t2").agg(
                functions.count("t1").as("t1"),
                functions.sum("t3").as("t3")
        ).as(tableModelEncoder);
        aggTable.show();
        List<JoinTable> aggList = aggTable.collectAsList();
        aggList.forEach(s-> System.out.println("im t1 "+ s.getT1()));

        spark.stop();
    }


}