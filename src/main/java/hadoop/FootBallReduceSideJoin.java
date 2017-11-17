package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.IOException;
import java.util.Iterator;

public class FootBallReduceSideJoin {
    private final static Text one = new Text("1");
    private static String[] fieldValues;
    private static String country;
    private static String countryWins;

    private static class FootballMembersMapper extends Mapper<Object, Text, TextPair, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().equals("name,club,age,position,position_cat,market_value,page_views,fpl_value,fpl_sel,fpl_points,region,nationality,new_foreign,age_cat,club_id,big_club,new_signing"))
                return;
            fieldValues = value.toString().split(",");
            if (fieldValues.length > 11)
                country = fieldValues[11];
            context.write(new TextPair(country, "1"), one);
        }

    }

    private static class FootballCountriesMapper extends Mapper<Object, Text, TextPair, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            fieldValues = value.toString().split(",");
            if (fieldValues.length > 1) {
                country = fieldValues[0];
                countryWins = fieldValues[1];
            }
            context.write(new TextPair(country, "0"), new Text(countryWins));
        }

    }

    private static class IntSumReducer extends Reducer<TextPair, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            country = key.getFirst().toString();
            countryWins = iterator.next().toString();
            if (!key.getSecond().toString().equals("0")) return;
            int sum = 0;
            while (iterator.hasNext()) {
                Text record = iterator.next();
                sum += Integer.valueOf(record.toString());
            }
            result.set("players: "+String.valueOf(sum));
            context.write(new Text(country+"\twins: "+countryWins), result);
        }
    }

    private static class KeyPartitioner extends Partitioner<TextPair, Text> {
        @Override
        public int getPartition(TextPair key, Text value, int numReduceTasks) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path outDir = new Path("output");
        hdfs.delete(outDir, true);
        if (hdfs.exists(outDir))
            return;
        Job job = Job.getInstance(conf, "football nationality count using Reduce-side join");
        job.setJarByClass(FootBallReduceSideJoin.class);
        MultipleInputs.addInputPath(job, new Path("epldata_final.csv"), TextInputFormat.class, FootballMembersMapper.class);
        MultipleInputs.addInputPath(job, new Path("champions.csv"), TextInputFormat.class, FootballCountriesMapper.class);
        FileOutputFormat.setOutputPath(job, outDir);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(TextPair.FirstComparator.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        Assert.assertTrue(job.waitForCompletion(true));
        FileStatus[] outputFiles = FileSystem.get(job.getConfiguration()).listStatus(outDir);
        for (FileStatus file : outputFiles) {
            System.out.println(file.getPath() + " " + file.getLen());
        }
    }
}

