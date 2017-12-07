package hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;

public class SparkRDD {
    public static void main(String[] args) throws URISyntaxException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkRDD")
                .master("local[2]")
                .getOrCreate();
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        fs.delete(new Path("sparkOutput"), true);
        JavaPairRDD<String, Integer> countries = spark
                .read()
                .textFile("champions.csv")
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<>(
                        s.split(",")[0],
                        Integer.valueOf(s.split(",")[1])
                ));
        JavaPairRDD<String, Integer> members = spark
                .read()
                .textFile("epldata_final.csv")
                .toJavaRDD()
                .mapToPair(s -> new Tuple2<>(
                        s.split(",")[11],
                        1
                )).reduceByKey((a, b) -> a + b);
        JavaPairRDD<String, Tuple2<Integer, Integer>> joined = countries.join(members);
        JavaPairRDD<Integer, Integer> reduced = joined
                .mapToPair(
                        s -> new Tuple2<>(
                                s._2._1, s._2._2
                        )
                ).reduceByKey((a, b) -> a + b).sortByKey(false);
        reduced.saveAsTextFile("sparkOutput");
        spark.stop();
    }
}
