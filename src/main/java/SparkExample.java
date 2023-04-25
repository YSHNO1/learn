import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkExample")
                .config("spark.master", "local")
                .getOrCreate();

        //使用spark进行数据处理
        Dataset<Row> df = spark.read().option("header", "true").csv("data.csv");
        df.show();

        //转换数据
        df = df.withColumn("age", df.col("age").plus(1));
        df.show();

        //聚合数据
        df.groupBy("gender").avg("age").show();
        spark.stop();
    }
}
