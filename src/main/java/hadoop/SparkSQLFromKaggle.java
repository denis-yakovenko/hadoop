package hadoop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;

public class SparkSQLFromKaggle {
    public static void main(String[] args) throws URISyntaxException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataSetSample")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> df = spark.sql("SELECT * FROM csv.`World-Happiness-Report-2017.csv`");
        Row header = df.first();
        df = df.filter(row -> row != header);
        df.show();
        Dataset<Row> df1 = spark.read().option("header", true).csv("World-Happiness-Report-2017.csv");
        df1.show();
        spark.stop();
    }
}
