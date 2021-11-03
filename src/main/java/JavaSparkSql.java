import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public final class JavaSparkSql {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
                .appName("JavaSpark")
                .getOrCreate();
        Dataset<Row> tables = spark.sql("Show databases;");
        tables.show();

        spark.stop();
    }


}