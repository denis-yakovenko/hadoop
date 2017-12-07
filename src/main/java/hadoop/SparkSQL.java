package hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URISyntaxException;

public class SparkSQL {
    public static void main(String[] args) throws URISyntaxException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkDataSetSample")
                .master("local[2]")
                .getOrCreate();
        FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
        fs.delete(new Path("sparkOutput"), true);
        Dataset<String> dataSet = spark
                .read()
                .textFile("epldata_final.csv")
                .cache();
        long brazilCount = dataSet.filter(s->s.contains("Brazil")).count();
        System.out.println(brazilCount);

        spark.stop();
    }
}
