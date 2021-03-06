package hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.net.URISyntaxException;

public class SparkRDD {
    private static SparkSession spark;
    private static FileSystem fs;

    private static void sparkInit() throws IOException {
        spark = SparkSession
                .builder()
                .appName("SparkRDD")
                .master("local[*]")
                .getOrCreate();
        fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
    }

    private static void sparkShutdown() {
        spark.stop();
    }


    public static void main(String[] args) throws URISyntaxException, IOException {
        sparkInit();
        example1();
        example2();
        example3();
        sparkShutdown();
    }

    private static void example3() {
        class GetLength implements Function<String, Integer> {
            public Integer call(String s) {
                return s.length();
            }
        }
        class Sum implements Function2<Integer, Integer, Integer> {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        }

        JavaRDD<String> lines = spark.read().textFile("epldata_final.csv").toJavaRDD();
        JavaRDD<Integer> lineLengths = lines.map(new GetLength());
        int totalLength = lineLengths.reduce(new Sum());
        System.out.println(totalLength);
    }

    private static void example2() throws URISyntaxException, IOException {
        JavaRDD<String> lines = spark.read().textFile("epldata_final.csv").toJavaRDD();
        JavaRDD<Integer> lineLengths = lines.map(new Function<String, Integer>() {
            public Integer call(String s) {
                return s.length();
            }
        });
        int totalLength = lineLengths.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        System.out.println(totalLength);
    }

    private static void example1() throws URISyntaxException, IOException {

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
    }
}
