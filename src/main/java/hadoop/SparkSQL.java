package hadoop;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;

import static org.apache.spark.sql.functions.desc;

public class SparkSQL {
    public static void main(String[] args) throws URISyntaxException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataSetSample")
                .master("local[2]")
                .getOrCreate();
        Dataset<Row> champions = spark.sql("SELECT * FROM csv.`champions.csv`").toDF("country", "wins");
        /*champions.show();*/
        Dataset<Row> members = spark.read().option("header", true).csv("epldata_final.csv");
        /*members
                .groupBy("nationality")
                .count()
                .orderBy(desc("count"))
                .show();*/
        champions
                .join(members, champions.col("country")
                        .equalTo(members.col("nationality")))
                .groupBy("wins")
                .count()
                .orderBy(desc("count"))
                .show();
        spark.stop();
    }
}
