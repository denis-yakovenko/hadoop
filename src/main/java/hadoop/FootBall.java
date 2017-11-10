package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FootBall {

    private static class FootballMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static Map<String, Integer> countries = new HashMap<>();

        static {
            countries.put("Brazil", 5);
            countries.put("Germany", 4);
            countries.put("Italy", 4);
            countries.put("Argentina", 3);
            countries.put("Uruguay", 2);
            countries.put("France", 1);
            countries.put("Spain", 1);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (value.toString().equals("name,club,age,position,position_cat,market_value,page_views,fpl_value,fpl_sel,fpl_points,region,nationality,new_foreign,age_cat,club_id,big_club,new_signing"))
                return;
            String[] fieldValues = value.toString().split(",");
            String country = fieldValues[11];
            if (!countries.containsKey(country)) {
                word.set(String.valueOf(0));
                context.write(word, one);
                return;
            }
            Integer winCount = countries.get(country);
            word.set(String.valueOf(winCount));
            context.write(word, one);
        }
    }

    private static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class KeyComparator extends WritableComparator {
        public KeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            if (a.toString().length() == b.toString().length())
                return a.toString().compareTo(b.toString());
            else return a.toString().length() - b.toString().length();
        }
    }

    private static class KeyPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            //return key.toString().matches("[a-kA-K](.*)") ? 0 : 1;
            return key.toString().length() < 6 ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        //Path inDir = new Path("input"/*args[0]*/);
        Path outDir = new Path("output"/*args[1]*/);
        hdfs.delete(outDir, true);
        if (hdfs.exists(outDir))
            return;
        Job job = Job.getInstance(conf, "football nationality count");
        job.setJarByClass(FootBall.class);
        job.setMapperClass(FootballMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //job.setPartitionerClass(KeyPartitioner.class);
        //job.setNumReduceTasks(2);
        //job.setSortComparatorClass(KeyComparator.class);
        //FileInputFormat.addInputPath(job, inDir);
        FileInputFormat.addInputPath(job, new Path("epldata_final.csv"));
        FileOutputFormat.setOutputPath(job, outDir);
        Assert.assertTrue(job.waitForCompletion(true));
        FileStatus[] outputFiles = FileSystem.get(job.getConfiguration()).listStatus(outDir);
        for (FileStatus file : outputFiles) {
            System.out.println(file.getPath() + " " + file.getLen());
        }
    }
}